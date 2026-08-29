package ingest

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sort"

	pb "device-telemetry-gateway/proto"
)

// FinalStateDigest reduces a set of Readings to one deterministic SHA-256
// digest: sort by (device_id, sequence), then hash the concatenation. Two
// independently produced Reading sets that reduce to the same digest are,
// for this project's purposes, "bit-identical": this is the function both
// the real pipeline's output and the reference oracle's expected output are
// run through before being compared.
func FinalStateDigest(readings []*pb.Reading) []byte {
	sorted := make([]*pb.Reading, len(readings))
	copy(sorted, readings)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].DeviceId != sorted[j].DeviceId {
			return sorted[i].DeviceId < sorted[j].DeviceId
		}
		return sorted[i].Sequence < sorted[j].Sequence
	})

	h := sha256.New()
	for _, r := range sorted {
		h.Write([]byte(r.DeviceId))
		var buf [17]byte
		binary.BigEndian.PutUint64(buf[0:8], r.Sequence)
		binary.BigEndian.PutUint64(buf[8:16], math.Float64bits(r.Value))
		h.Write(buf[:])
	}
	return h.Sum(nil)
}

// ReferenceDedup is a deliberately naive, independently written
// deduplication used only to build ground truth in tests and the benchmark:
// given a raw, possibly duplicated and out-of-order delivery sequence,
// return one Reading per unique (device_id, sequence), first occurrence
// wins. It shares no code with Dedup or Pipeline, so a bug common to both of
// those could not also be present here by construction.
func ReferenceDedup(delivered []*pb.Reading) []*pb.Reading {
	type key struct {
		device string
		seq    uint64
	}
	seen := make(map[key]bool)
	out := make([]*pb.Reading, 0, len(delivered))
	for _, r := range delivered {
		k := key{r.DeviceId, r.Sequence}
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, r)
	}
	return out
}
