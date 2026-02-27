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
	"log"

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

	// Implementation note: Arrow Flight RecordReader is usually the way to go
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return err
	}
	defer reader.Release()

	// Create batch handler for large record batches
	handler, err := NewParquetBatchHandler()
	if err != nil {
		return fmt.Errorf("failed to create batch handler: %v", err)
	}
	defer handler.Cleanup()

	// Process records and write to parquet files if size exceeds 1GB
	var records []arrow.RecordBatch
	var totalSize int64
	const maxBatchSize = 1000 * 1000 // 1 million rows

	for reader.Next() {
		rec := reader.RecordBatch()
		rec.Retain()
		records = append(records, rec)

		log.Printf("Received record batch with size of %d", rec.NumRows())

		// Calculate size of this record batch (approximate by row count)
		totalSize += rec.NumRows()

		// If total size exceeds 1GB, write to parquet and reset
		if totalSize >= maxBatchSize {
			log.Printf("Total size of records (%d rows) exceeds threshold, writing to parquet", totalSize)

			// Write current records to parquet file
			for _, r := range records {
				if err := handler.AddRecordBatch(r); err != nil {
					return fmt.Errorf("failed to write record batch to parquet: %v", err)
				}
			}
			// Release current records and reset
			for _, r := range records {
				r.Release()
			}
			records = nil
			totalSize = 0
		}
	}

	if err := reader.Err(); err != nil {
		return err
	}

	var events *route_event.LRSEvents
	var err2 error

	// If we wrote parquet files, merge them and create LRSEvents from file
	if len(handler.currentFiles) > 0 {
		// Write any remaining records to parquet
		for _, r := range records {
			if err := handler.AddRecordBatch(r); err != nil {
				return fmt.Errorf("failed to write remaining record batch to parquet: %v", err)
			}
		}
		// Release remaining records
		for _, r := range records {
			r.Release()
		}
		records = nil

		// Merge all parquet files
		mergedPath, err := handler.MergeParquetFiles()
		if err != nil {
			return fmt.Errorf("failed to merge parquet files: %v", err)
		}

		// Create LRSEvents from merged parquet file
		events, err2 = route_event.NewLRSEventsFromFile(mergedPath, crs)
		if err2 != nil {
			return fmt.Errorf("error when creating LRSEvents from file: %v", err2)
		}
	} else {
		// No parquet files created, use records directly
		if len(records) == 0 {
			return fmt.Errorf("no records received")
		}

		// Create LRSEvents from records
		events, err2 = route_event.NewLRSEvents(records, crs)
		if err2 != nil {
			return fmt.Errorf("error when creating new LRSEvents: %v", err2)
		}
		defer events.Release()
	}

	// Check if events need to be materialized (loaded from file)
	if events.IsMaterialized() {
		if err := events.LoadToBuffer(); err != nil {
			return fmt.Errorf("failed to materialize events from file: %v", err)
		}
	}

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

	fmt.Printf("Found %d unique route IDs\n", len(routeIDs))

	// Create a batch to handle multiple routes
	routeBatch := &route.LRSRouteBatch{}
	routesLoaded := 0

	// Load all LRS routes
	for _, routeID := range routeIDs {
		lrs, err := s.repo.GetLatest(ctx, routeID)
		if err != nil {
			fmt.Printf("Warning: failed to get LRS route for %s: %v (skipping)\n", routeID, err)
			continue
		}
		defer lrs.Release()

		if err := routeBatch.AddRoute(*lrs); err != nil {
			fmt.Printf("Warning: failed to add route %s to batch: %v (skipping)\n", routeID, err)
			lrs.Release()
			continue
		}
		routesLoaded++
	}

	if routesLoaded == 0 {
		return fmt.Errorf("failed to load any LRS routes for %d route IDs", len(routeIDs))
	}

	fmt.Printf("Successfully loaded %d LRS routes into batch\n", routesLoaded)

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
