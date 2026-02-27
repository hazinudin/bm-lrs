package flight

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ParquetBatchHandler handles writing record batches to parquet files and merging them
type ParquetBatchHandler struct {
	tempDir      string
	currentFiles []string
	schema       *arrow.Schema
	totalSize    int64
	batchIndex   int
}

// NewParquetBatchHandler creates a new handler for managing parquet file batches
func NewParquetBatchHandler() (*ParquetBatchHandler, error) {
	tempDir, err := os.MkdirTemp("", "lrs_flight_batch_*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary directory: %v", err)
	}

	return &ParquetBatchHandler{
		tempDir:    tempDir,
		totalSize:  0,
		batchIndex: 0,
	}, nil
}

// AddRecordBatch adds a record batch to the handler. If the total size exceeds 1GB,
// it writes the current batch to a parquet file and starts a new batch.
func (h *ParquetBatchHandler) AddRecordBatch(rec arrow.RecordBatch) error {
	const maxBatchSize = 1024 * 1024 * 1024 // 1GB

	// Set schema from first batch
	if h.schema == nil {
		h.schema = rec.Schema()
	}

	// Add the record batch size
	batchSize := rec.NumRows()
	h.totalSize += batchSize

	// Create a temporary file for this batch
	h.batchIndex++
	filePath := filepath.Join(h.tempDir, fmt.Sprintf("batch_%d.parquet", h.batchIndex))

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file: %v", err)
	}
	defer f.Close()

	writer, err := pqarrow.NewFileWriter(
		h.schema,
		f,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy)),
		pqarrow.DefaultWriterProps(),
	)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %v", err)
	}
	defer writer.Close()

	if err := writer.WriteBuffered(rec); err != nil {
		return fmt.Errorf("failed to write record batch: %v", err)
	}
	log.Printf("Wrote batch %d with %d rows to %s", h.batchIndex, batchSize, filePath)

	h.currentFiles = append(h.currentFiles, filePath)

	// If total size exceeds 1GB, reset for next batch
	if h.totalSize >= maxBatchSize {
		h.totalSize = 0
	}

	return nil
}

// MergeParquetFiles merges all parquet files into a single parquet file
func (h *ParquetBatchHandler) MergeParquetFiles() (string, error) {
	if len(h.currentFiles) == 0 {
		return "", fmt.Errorf("no parquet files to merge")
	}

	mergedPath := filepath.Join(h.tempDir, fmt.Sprintf("merged_%d.parquet", time.Now().UnixNano()))

	mergedFile, err := os.Create(mergedPath)
	if err != nil {
		return "", fmt.Errorf("failed to create merged parquet file: %v", err)
	}
	defer mergedFile.Close()

	// Read the first file to get the correct schema (with metadata)
	var firstSchema *arrow.Schema
	for _, filePath := range h.currentFiles {
		pf, err := file.OpenParquetFile(filePath, false)
		if err != nil {
			return "", fmt.Errorf("failed to open parquet file %s: %v", filePath, err)
		}

		reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{
			BatchSize: 10000,
		}, memory.NewGoAllocator())
		if err != nil {
			pf.Close()
			return "", fmt.Errorf("failed to create arrow reader for %s: %v", filePath, err)
		}

		firstSchema, _ = reader.Schema()
		pf.Close()
		break
	}

	// Create writer for merged file using the schema from the first file (with metadata)
	mergedWriter, err := pqarrow.NewFileWriter(
		h.schema,
		mergedFile,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy)),
		pqarrow.DefaultWriterProps(),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create merged parquet writer: %v", err)
	}
	defer mergedWriter.Close()

	// Read and write all batches from all files
	for _, filePath := range h.currentFiles {
		// Use file.OpenParquetFile to open the parquet file
		pf, err := file.OpenParquetFile(filePath, false)
		if err != nil {
			return "", fmt.Errorf("failed to open parquet file %s: %v", filePath, err)
		}

		// Create arrow reader from parquet file
		reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{
			BatchSize: 10000,
		}, memory.NewGoAllocator())
		if err != nil {
			pf.Close()
			return "", fmt.Errorf("failed to create arrow reader for %s: %v", filePath, err)
		}

		// Create a record reader for this file
		recordReader, err := reader.GetRecordReader(context.Background(), nil, nil)
		if err != nil {
			pf.Close()
			return "", fmt.Errorf("failed to get record reader for %s: %v", filePath, err)
		}

		// Read all records from this file and write to merged file
		for recordReader.Next() {
			rec := recordReader.RecordBatch()
			if err := mergedWriter.WriteBuffered(rec); err != nil {
				recordReader.Release()
				pf.Close()
				return "", fmt.Errorf("failed to write record to merged file: %v", err)
			}
		}

		if err := recordReader.Err(); err != nil {
			recordReader.Release()
			pf.Close()
			return "", fmt.Errorf("error reading records from %s: %v", filePath, err)
		}

		recordReader.Release()
		pf.Close()
	}

	return mergedPath, nil
}

// Cleanup removes the temporary directory and all files
func (h *ParquetBatchHandler) Cleanup() error {
	if h.tempDir != "" {
		return os.RemoveAll(h.tempDir)
	}
	return nil
}
