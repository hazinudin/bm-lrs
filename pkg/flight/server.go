package flight

import (
	"bm-lrs/pkg/mvalue"
	"bm-lrs/pkg/route"
	"bm-lrs/pkg/route_event"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type LRSFlightServer struct {
	flight.BaseFlightServer
	repo *route.LRSRouteRepository
}

func NewLRSFlightServer(repo *route.LRSRouteRepository) *LRSFlightServer {
	return &LRSFlightServer{
		repo: repo,
	}
}

func (s *LRSFlightServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	desc, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}

	// The first message might only contain metadata or descriptor
	// If it's a DoExchange, the first message usually has the FlightDescriptor
	// But in some implementations, it's just FlightData.
	// Let's check for AppMetadata in the first message.

	operation := string(desc.AppMetadata)
	if operation == "" {
		// Try to see if it's in the descriptor
		// For now, let's assume it's in AppMetadata of the first FlightData
	}

	switch operation {
	case "calculate_m_value":
		return s.handleCalculateMValue(stream, desc)
	default:
		return fmt.Errorf("unsupported operation: %s", operation)
	}
}

func (s *LRSFlightServer) handleCalculateMValue(stream flight.FlightService_DoExchangeServer, firstData *flight.FlightData) error {
	ctx := stream.Context()

	// Read record batches from the stream
	var records []arrow.RecordBatch

	// If the first message had data, process it
	if len(firstData.DataBody) > 0 {
		// We need to use a flight.Reader to parse the data
		// This is a bit tricky since we are already in the middle of a stream
		// Usually, we use flight.NewRecordReader(stream) but that consumes the stream
	}

	// Implementation note: Arrow Flight RecordReader is usually the way to go
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return err
	}
	defer reader.Release()

	// Collect all records to form an LRSEvents object
	// Note: In a real production scenario, we might want to process batch by batch
	// but CalculatePointsMValue currently takes all records.
	for reader.Next() {
		rec := reader.RecordBatch()
		rec.Retain()
		records = append(records, rec)
	}

	if len(records) == 0 {
		return fmt.Errorf("no records received")
	}

	// Create LRSEvents
	events, err := route_event.NewLRSEvents(records, "EPSG:4326") // Default CRS
	if err != nil {
		return err
	}
	defer events.Release()

	// Get Route IDs from events to fetch LRS data
	routeIDs := events.GetRouteIDs()
	if len(routeIDs) == 0 {
		return fmt.Errorf("no ROUTEID found in records")
	}

	// For now, assume one route ID per exchange or handle the first one
	// Ideally we handle multiple routes if needed, but CalculatePointsMValue takes one LRS route.
	routeID := routeIDs[0]
	lrs, err := s.repo.GetLatest(ctx, routeID)
	if err != nil {
		return fmt.Errorf("failed to get LRS route for %s: %v", routeID, err)
	}
	defer lrs.Release()

	// Calculate M-Values
	resultEvents, err := mvalue.CalculatePointsMValue(ctx, lrs, *events)
	if err != nil {
		return fmt.Errorf("failed to calculate m-values: %v", err)
	}
	defer resultEvents.Release()

	// Stream back the results
	// Use flight.Writer to handle the complexity of Arrow Flight data framing
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(resultEvents.GetRecords()[0].Schema()))
	defer writer.Close()

	for _, rec := range resultEvents.GetRecords() {
		if err := writer.Write(rec); err != nil {
			return err
		}
	}
	return nil
}
