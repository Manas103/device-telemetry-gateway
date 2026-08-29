package ingest

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "device-telemetry-gateway/proto"
)

// Spool is an append-only, length-prefixed file of protobuf Readings. It is
// where a Reading goes when the downstream Kafka path cannot take it right
// now (a full producer, or the broker being unreachable), so backpressure
// degrades into "written to disk, forward it later" instead of "dropped".
type Spool struct {
	mu   sync.Mutex
	file *os.File
	w    *bufio.Writer
}

func NewSpool(path string) (*Spool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return &Spool{file: f, w: bufio.NewWriter(f)}, nil
}

func (s *Spool) Write(r *pb.Reading) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, err := proto.Marshal(r)
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
	if _, err := s.w.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	return s.w.Flush()
}

// Drain reads every record currently in the spool file, truncates it, and
// returns the records for the caller to retry. The lock is held for the
// whole read-then-truncate so a concurrent Write can never land between the
// read and the truncate and be silently discarded.
func (s *Spool) Drain() ([]*pb.Reading, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.w.Flush(); err != nil {
		return nil, err
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	r := bufio.NewReader(s.file)
	var out []*pb.Reading
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			break // EOF or a short trailing write: end of usable spool content
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			break
		}
		var reading pb.Reading
		if err := proto.Unmarshal(buf, &reading); err != nil {
			continue // corrupt record: skip rather than abort the whole drain
		}
		out = append(out, &reading)
	}

	if err := s.file.Truncate(0); err != nil {
		return nil, err
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	s.w = bufio.NewWriter(s.file)

	return out, nil
}

// SizeBytes reports how much unflushed content currently sits in the spool
// file, without draining it. Used to poll "has the backlog been cleared yet"
// after an outage, rather than guessing a fixed wait: a large backlog
// produced during a long outage takes a while to drain one record at a time,
// and that time scales with the backlog, not with the outage duration.
func (s *Spool) SizeBytes() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.Flush(); err != nil {
		return 0, err
	}
	info, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
