package api

import (
	"bm-lrs/pkg/geom"
	"bm-lrs/pkg/mvalue"
	"bm-lrs/pkg/projection"
	"bm-lrs/pkg/route"
	"bm-lrs/pkg/route_event"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// APIHandler handles REST API requests for M-Value calculation
type APIHandler struct {
	repo *route.LRSRouteRepository
}

// NewAPIHandler creates a new APIHandler
func NewAPIHandler(repo *route.LRSRouteRepository) *APIHandler {
	return &APIHandler{
		repo: repo,
	}
}

// CalculateMValueRequest represents the request for M-Value calculation
type CalculateMValueRequest struct {
	CRS string `json:"crs"` // Optional, defaults to EPSG:4326
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}

// CalculateMValueHandler handles POST requests to calculate M-Value from GeoJSON
func (h *APIHandler) CalculateMValueHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.sendError(w, http.StatusMethodNotAllowed, "only POST method is allowed")
		return
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, fmt.Sprintf("failed to read request body: %v", err))
		return
	}
	defer r.Body.Close()

	// Get CRS from query parameter, default to EPSG:4326
	crs := r.URL.Query().Get("crs")
	if crs == "" {
		crs = "EPSG:4326"
	}

	// Validate GeoJSON structure
	if err := h.validateGeoJSON(body); err != nil {
		h.sendError(w, http.StatusBadRequest, fmt.Sprintf("invalid GeoJSON: %v", err))
		return
	}

	// Create LRSEvents from GeoJSON
	events, err := route_event.NewLRSEventsFromGeoJSON(body, crs)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, fmt.Sprintf("failed to parse GeoJSON: %v", err))
		return
	}
	defer events.Release()

	// Transform to Lambert CRS if needed
	var processedEvents *route_event.LRSEvents
	if events.GetCRS() != geom.LAMBERT_WKT {
		transformedGeom, err := projection.Transform(events, geom.LAMBERT_WKT, false)
		if err != nil {
			h.sendError(w, http.StatusInternalServerError, fmt.Sprintf("failed to transform projection: %v", err))
			return
		}
		defer transformedGeom.Release()

		processedEvents, err = route_event.NewLRSEvents(transformedGeom.GetRecords(), geom.LAMBERT_WKT)
		if err != nil {
			h.sendError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create LRSEvents after transformation: %v", err))
			return
		}
	} else {
		processedEvents = events
	}

	// Get Route IDs from events
	routeIDs := processedEvents.GetRouteIDs()
	if len(routeIDs) == 0 {
		h.sendError(w, http.StatusBadRequest, "no ROUTEID found in GeoJSON properties")
		return
	}

	// For now, process the first route (could be extended to handle multiple routes)
	routeID := routeIDs[0]
	lrs, err := h.repo.GetLatest(r.Context(), routeID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, fmt.Sprintf("failed to get LRS route for %s: %v", routeID, err))
		return
	}
	defer lrs.Release()

	// Calculate M-Values
	resultEvents, err := mvalue.CalculatePointsMValue(r.Context(), lrs, *processedEvents)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, fmt.Sprintf("failed to calculate m-values: %v", err))
		return
	}
	defer resultEvents.Release()

	// Convert result to GeoJSON
	geojsonBytes, err := resultEvents.ToGeoJSON()
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, fmt.Sprintf("failed to serialize result to GeoJSON: %v", err))
		return
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(geojsonBytes)
}

// validateGeoJSON validates the basic GeoJSON structure
func (h *APIHandler) validateGeoJSON(data []byte) error {
	var fc struct {
		Type     string `json:"type"`
		Features []struct {
			Type     string `json:"type"`
			Geometry struct {
				Type        string    `json:"type"`
				Coordinates []float64 `json:"coordinates"`
			} `json:"geometry"`
			Properties map[string]interface{} `json:"properties"`
		} `json:"features"`
	}

	if err := json.Unmarshal(data, &fc); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	if fc.Type != "FeatureCollection" {
		return fmt.Errorf("expected FeatureCollection, got %s", fc.Type)
	}

	if len(fc.Features) == 0 {
		return fmt.Errorf("no features in FeatureCollection")
	}

	for i, f := range fc.Features {
		if f.Type != "Feature" {
			return fmt.Errorf("feature %d: expected Feature type, got %s", i, f.Type)
		}
		if f.Geometry.Type != "Point" {
			return fmt.Errorf("feature %d: only Point geometry is supported, got %s", i, f.Geometry.Type)
		}
		if len(f.Geometry.Coordinates) < 2 {
			return fmt.Errorf("feature %d: Point must have at least 2 coordinates", i)
		}
		if _, ok := f.Properties["ROUTEID"]; !ok {
			return fmt.Errorf("feature %d: missing required ROUTEID property", i)
		}
	}

	return nil
}

// sendError sends an error response as JSON
func (h *APIHandler) sendError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}
