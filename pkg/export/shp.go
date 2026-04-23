package export

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
)

func ExportToSHP(ctx context.Context, db *sql.DB, selectQuery string) ([]byte, error) {
	tempDir, err := os.MkdirTemp("", "shp_export_*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	shpPath := filepath.Join(tempDir, "routes.shp")

	copyQuery := fmt.Sprintf(`
		install spatial;
		load spatial;
		COPY (%s)
		TO '%s'
		WITH (FORMAT GDAL, DRIVER 'ESRI Shapefile')
	`, selectQuery, shpPath)

	if _, err := db.ExecContext(ctx, copyQuery); err != nil {
		return nil, fmt.Errorf("failed to export to shapefile: %w", err)
	}

	var shpFiles []string
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read temp dir: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		ext := filepath.Ext(name)
		if ext == ".shp" || ext == ".shx" || ext == ".dbf" || ext == ".prj" {
			shpFiles = append(shpFiles, filepath.Join(tempDir, name))
		}
	}

	if len(shpFiles) == 0 {
		return nil, fmt.Errorf("no shapefile components generated")
	}

	zipBuf := new(bytes.Buffer)
	buf := zip.NewWriter(zipBuf)
	for _, fPath := range shpFiles {
		fData, err := os.ReadFile(fPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", fPath, err)
		}

		w, err := buf.Create(filepath.Base(fPath))
		if err != nil {
			return nil, fmt.Errorf("failed to create zip entry: %w", err)
		}
		if _, err := w.Write(fData); err != nil {
			return nil, fmt.Errorf("failed to write %s to zip: %w", fPath, err)
		}
	}

	if err := buf.Close(); err != nil {
		return nil, fmt.Errorf("failed to finalize zip: %w", err)
	}

	return zipBuf.Bytes(), nil
}
