package flight

import (
	"bm-lrs/pkg/geom"
	"bm-lrs/pkg/mvalue"
	"bm-lrs/pkg/projection"
	"bm-lrs/pkg/route"
	"bm-lrs/pkg/route_event"
	"encoding/json"
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

	// Parse the operation and CRS from AppMetadata
	var operation string
	crs := "EPSG:4326" // Default CRS

	// Define a struct for the expected JSON metadata
	type Action struct {
		Operation string `json:"operation"`
		CRS       string `json:"crs"`
	}

	var action Action
	// Try parsing as JSON first
	if len(desc.AppMetadata) > 0 {
		if err := json.Unmarshal(desc.AppMetadata, &action); err == nil && action.Operation != "" {
			operation = action.Operation
			if action.CRS != "" {
				crs = action.CRS
			}
		} else {
			// Fallback: treat the metadata as a raw string (the operation name)
			operation = string(desc.AppMetadata)
		}
	} else if desc.FlightDescriptor != nil && len(desc.FlightDescriptor.Cmd) > 0 {
		// Try parsing from Descriptor.Cmd
		if err := json.Unmarshal(desc.FlightDescriptor.Cmd, &action); err == nil && action.Operation != "" {
			operation = action.Operation
			if action.CRS != "" {
				crs = action.CRS
			}
		} else {
			operation = string(desc.FlightDescriptor.Cmd)
		}
	}

	fmt.Printf("Operation: %s, CRS: %s\n", operation, crs)

	switch operation {
	case "calculate_m_value":
		return s.handleCalculateMValue(stream, desc, crs)
	default:
		return fmt.Errorf("unsupported operation: %s", operation)
	}
}

func (s *LRSFlightServer) handleCalculateMValue(stream flight.FlightService_DoExchangeServer, firstData *flight.FlightData, crs string) error {
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
	for reader.Next() {
		rec := reader.RecordBatch()
		rec.Retain()
		records = append(records, rec)
	}

	if err := reader.Err(); err != nil {
		return err
	}

	if len(records) == 0 {
		return fmt.Errorf("no records received")
	}

	// Create LRSEvents
	events, err := route_event.NewLRSEvents(records, crs)
	if err != nil {
		return fmt.Errorf("error when creating new LRSEvents: %v", err)
	}
	defer events.Release()

	// Check the Events CRS
	if events.GetCRS() != geom.LAMBERT_WKT {
		transformedEvents, err := projection.Transform(events, geom.LAMBERT_WKT, false)
		if err != nil {
			return err
		}
		defer transformedEvents.Release()

		events, err = route_event.NewLRSEvents(transformedEvents.GetRecords(), geom.LAMBERT_WKT)
		if err != nil {
			return fmt.Errorf("error creating LRSEvents after transformation: %v", err)
		}
	}

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
