package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCalculateMValueHandler_InvalidMethod(t *testing.T) {
	handler := NewAPIHandler(nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/calculate-mvalue", nil)
	rr := httptest.NewRecorder()

	handler.CalculateMValueHandler(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, rr.Code)
	}
}

func TestCalculateMValueHandler_InvalidGeoJSON(t *testing.T) {
	handler := NewAPIHandler(nil)

	body := bytes.NewBufferString(`{"invalid": "json"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/calculate-mvalue", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler.CalculateMValueHandler(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusBadRequest, rr.Code)
	}
}

func TestCalculateMValueHandler_InvalidGeoJSONType(t *testing.T) {
	handler := NewAPIHandler(nil)

	body := bytes.NewBufferString(`{"type": "Feature", "geometry": {"type": "Point", "coordinates": [1,2]}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/calculate-mvalue", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler.CalculateMValueHandler(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusBadRequest, rr.Code)
	}
}

func TestCalculateMValueHandler_MissingRouteID(t *testing.T) {
	handler := NewAPIHandler(nil)

	body := bytes.NewBufferString(`{
		"type": "FeatureCollection",
		"features": [{
			"type": "Feature",
			"geometry": {"type": "Point", "coordinates": [95.35, 5.50]},
			"properties": {"name": "test"}
		}]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/calculate-mvalue", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler.CalculateMValueHandler(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusBadRequest, rr.Code)
	}
}

func TestValidateGeoJSON_ValidInput(t *testing.T) {
	handler := NewAPIHandler(nil)

	validGeoJSON := []byte(`{
		"type": "FeatureCollection",
		"features": [{
			"type": "Feature",
			"geometry": {"type": "Point", "coordinates": [95.35, 5.50]},
			"properties": {"ROUTEID": "01002"}
		}]
	}`)

	err := handler.validateGeoJSON(validGeoJSON)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
}

func TestValidateGeoJSON_NonPointGeometry(t *testing.T) {
	handler := NewAPIHandler(nil)

	invalidGeoJSON := []byte(`{
		"type": "FeatureCollection",
		"features": [{
			"type": "Feature",
			"geometry": {"type": "LineString", "coordinates": [[95.35, 5.50], [95.36, 5.51]]},
			"properties": {"ROUTEID": "01002"}
		}]
	}`)

	err := handler.validateGeoJSON(invalidGeoJSON)
	if err == nil {
		t.Error("Expected error for non-Point geometry, got nil")
	}
}
