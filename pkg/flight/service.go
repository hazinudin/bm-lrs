package flight

import (
	"bm-lrs/pkg/route"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
)

func NewFlightServer(repo *route.LRSRouteRepository) flight.Server {
	server := flight.NewServerWithMiddleware(nil)
	lrsServer := NewLRSFlightServer(repo)
	server.RegisterFlightService(lrsServer)
	return server
}

func StartFlightServer(repo *route.LRSRouteRepository, port int) error {
	addr := fmt.Sprintf(":%d", port)
	server := NewFlightServer(repo)
	log.Printf("Starting LRS Flight server on %s...\n", addr)
	if err := server.Init(addr); err != nil {
		return err
	}
	return server.Serve()
}

// StartFlightServerWithGRPC allows passing custom gRPC options
func StartFlightServerWithGRPC(repo *route.LRSRouteRepository, port int, opts ...grpc.ServerOption) error {
	addr := fmt.Sprintf(":%d", port)
	server := flight.NewServerWithMiddleware(nil, opts...)
	lrsServer := NewLRSFlightServer(repo)
	server.RegisterFlightService(lrsServer)

	log.Printf("Starting LRS Flight server on %s...\n", addr)
	if err := server.Init(addr); err != nil {
		return err
	}
	return server.Serve()
}
