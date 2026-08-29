// Package wsserver is the device-facing edge: one WebSocket connection per
// simulated device, each binary frame a single serialized protobuf Reading.
package wsserver

import (
	"context"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"device-telemetry-gateway/internal/ingest"
	pb "device-telemetry-gateway/proto"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// Server accepts one WebSocket connection per simulated device and feeds
// every protobuf-framed Reading it receives into the ingest pipeline.
type Server struct {
	pipeline *ingest.Pipeline
}

func NewServer(p *ingest.Pipeline) *Server {
	return &Server{pipeline: p}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", s.handleIngest)
	return mux
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return // connection closed by the simulated device; nothing left to read
		}
		var reading pb.Reading
		if err := proto.Unmarshal(data, &reading); err != nil {
			log.Printf("wsserver: dropping unparseable frame: %v", err)
			continue
		}
		if err := s.pipeline.Ingest(context.Background(), &reading); err != nil {
			log.Printf("wsserver: ingest failed for %s/%d: %v", reading.DeviceId, reading.Sequence, err)
		}
	}
}
