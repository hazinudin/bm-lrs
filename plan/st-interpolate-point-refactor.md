# Implementation Plan: Refactor to Use ST_InterpolatePoint

## Summary

Refactor M-value interpolation system to leverage DuckDB Spatial's built-in `ST_InterpolatePoint` function instead of current complex multi-CTE approach. This will simplify code, improve maintainability, and potentially enhance performance.

**Current Version:** duckdb-go v2.5.0  
**Target Feature:** ST_InterpolatePoint (available in DuckDB Spatial v1.4-andium+)  
**Feature Flag:** Enabled for gradual rollout with fallback to original implementation

---

## Phase 1: Environment & Dependencies

### Task 1.1: Verify and Update DuckDB Spatial Extension
- **Agent:** general
- **Action:** Check current DuckDB version and spatial extension version
- **Steps:**
  1. Run DuckDB query: `SELECT duckdb_spatial_version()` or `SELECT extension_version('spatial')`
  2. Check if v1.4-andium or later is available
  3. If not available, research how to install latest spatial extension in DuckDB
  4. Document required DuckDB spatial version
- **Deliverable:** Report on current/required DuckDB spatial version and installation instructions

### Task 1.2: Update go.mod Dependencies
- **Agent:** general
- **Action:** Ensure duckdb-go v2.5.0 supports latest spatial extension
- **Steps:**
  1. Check duckdb-go v2.5.0 release notes for spatial extension compatibility
  2. Verify no conflicting dependency versions
  3. If needed, identify required version updates
- **Deliverable:** Confirmation that current dependencies support ST_InterpolatePoint

---

## Phase 2: Core Implementation

### Task 2.1: Add Feature Flag Infrastructure
- **Agent:** general
- **Action:** Add configuration toggle for ST_InterpolatePoint
- **Steps:**
  1. Create new file: `pkg/config/feature_flags.go` (or add to existing config)
  2. Add `UseSTInterpolatePoint` bool flag (default: true for new installs)
  3. Add environment variable support (e.g., `ST_INTERPOLATE_POINT_ENABLED=true`)
  4. Add getter function to check flag
- **Files to create/modify:**
  - `pkg/config/feature_flags.go` (new)
- **Deliverable:** Feature flag infrastructure in place

### Task 2.2: Refactor LinestringQuery() to Create M-enabled Linestrings
- **Agent:** general
- **Action:** Update LRS route linestring queries to include M-dimension using WKT construction
- **Steps:**
  1. Modify `pkg/route/route.go:270-307` (LinestringQuery method)
  2. Change from:
     ```sql
     SELECT {{.RouteID}} as ROUTEID, ST_MakeLine(
       list(ST_Point({{.LatCol}}, {{.LongCol}}) order by {{.VertexSeqCol}} asc)
     ) as linestr 
     FROM {{.ViewName}}
     ```
  3. To:
     ```sql
     SELECT {{.RouteID}} as ROUTEID,
       ST_GeomFromText(
         'LINESTRING ZM (' || 
           STRING_AGG(
             CONCAT({{.LongCol}}, ' ', {{.LatCol}}, ' 0 ', {{.MvalCol}}), 
             ', ' 
             ORDER BY {{.VertexSeqCol}} ASC
           ) || 
         ')'
       ) as linestr
     FROM {{.ViewName}}
     ```
  4. Note: Z coordinate is set to 0 (not used, but required for ZM format)
  5. Format: `longitude latitude 0 mvalue` per vertex
  6. Review `pkg/route/batch.go:220-251` (LRSRouteBatch.LinestringQuery)
     - This method queries existing materialized linestring files
     - Query construction uses `SELECT * FROM "file"` which doesn't construct geometry
     - No changes needed to query (materialized data regenerated in Task 3.3)
- **Files to modify:**
  - `pkg/route/route.go`
- **Deliverable:** LRSRoute.LinestringQuery() creates M-enabled linestrings

### Task 2.3: Implement ST_InterpolatePoint Version
- **Agent:** general
- **Action:** Create new CalculatePointsMValue implementation
- **Steps:**
  1. In `pkg/mvalue/mvalue.go`, add new function `calculatePointsMValueNew()`
  2. Remove CTEs (lines 136-179) and replace with simplified query:
     ```sql
     SELECT 
       p.* EXCLUDE ({excludeClause}),
       ST_InterpolatePoint(b.linestr, ST_Point("{latCol}", "{lonCol}")) as "{mvalCol}",
       ST_Distance(b.linestr, ST_Point("{latCol}", "{lonCol}")) as dist_to_line
     FROM points_table p
     JOIN lrs_line_table b ON p.ROUTEID = b.ROUTEID
     ORDER BY p.point_id
     ```
  3. Remove manual interpolation logic (line 174)
  4. Remove bounding box comparisons (lines 166-168)
  5. Keep all existing error handling and debug logging
  6. Maintain backward compatible output schema
- **Files to modify:**
  - `pkg/mvalue/mvalue.go`
- **Deliverable:** New simplified calculatePointsMValueNew() function

### Task 2.4: Integrate Feature Flag
- **Agent:** general
- **Action:** Modify CalculatePointsMValue to toggle between implementations
- **Steps:**
  1. Modify `CalculatePointsMValue()` in `pkg/mvalue/mvalue.go`
  2. Add check for feature flag at start of function:
     ```go
     func CalculatePointsMValue(ctx context.Context, lrs route.LRSRouteInterface, points route_event.LRSEvents) (*route_event.LRSEvents, error) {
         // Check feature flag at runtime
         if featureFlags.UseSTInterpolatePoint() {
             // Use NEW: Simplified ST_InterpolatePoint implementation
             return calculatePointsMValueNew(ctx, lrs, points)
         }
         
         // Use ORIGINAL: Complex CTE-based implementation (unchanged)
         return calculatePointsMValueOriginal(ctx, lrs, points)
     }
     ```
  3. Rename existing implementation to `calculatePointsMValueOriginal()`
  4. Ensure flag is accessible in CalculatePointsMValue context
  5. Add comment: `// DEPRECATED: Kept for backward compatibility via feature flag`
- **Files to modify:**
  - `pkg/mvalue/mvalue.go`
- **Deliverable:** Toggleable M-value calculation based on feature flag

---

## Phase 3: Test Data Regeneration

### Task 3.1: Identify All Test Parquet Files
- **Agent:** general
- **Action:** Find all materialized test data files
- **Steps:**
  1. Search for `.parquet` files in `pkg/route/testdata/` directories
  2. Search for linestring references in test files
  3. Identify which files are generated by LinestringQuery()
- **Expected files to examine:**
  - `pkg/route/testdata/lrs_01001_linestr.parquet` (linestring - NEEDS REGENERATION)
  - `pkg/route/testdata/lrs_01001_point.parquet` (raw vertex data - NO CHANGE)
  - `pkg/route/testdata/lrs_01001_segment.parquet` (segment data - NO CHANGE)
- **Deliverable:** List of test parquet files and their regeneration status

### Task 3.2: Verify Current Linestring Schema
- **Agent:** general
- **Action:** Check current linestring parquet structure
- **Steps:**
  1. Run: `duckdb -c "INSTALL spatial; LOAD spatial; SELECT ST_HasM(linestr) FROM 'pkg/route/testdata/lrs_01001_linestr.parquet'"`
  2. Document current M-dimension status (expected: false)
  3. Verify schema columns (ROUTEID, linestr BLOB)
  4. Store baseline for comparison
- **Deliverable:** Baseline documentation of current linestring structure

### Task 3.3: Regenerate Route Test Linestring Parquet File
- **Agent:** general
- **Action:** Run LinestringQuery test to regenerate parquet file
- **Prerequisites:**
  - Phase 2.2 must be complete (LinestringQuery uses ST_Force3DM)
  - Phase 2.4 must be complete (feature flag enabled)
- **Steps:**
  1. Run test in `pkg/route/route_test.go:230-256` ("linestring table view test")
  2. This executes:
     ```go
     _, err = db.QueryContext(
         context.Background(), 
         fmt.Sprintf(
             "copy (%s) to 'testdata/lrs_01001_linestr.parquet' (FORMAT parquet);", 
             lrs.LinestringQuery()  // ← Uses new ST_Force3DM implementation
         )
     )
     ```
  3. Verify `pkg/route/testdata/lrs_01001_linestr.parquet` is overwritten
  4. Check file size (may increase slightly due to M-dimension)
- **Files affected:**
  - `pkg/route/testdata/lrs_01001_linestr.parquet` (regenerated)
- **Deliverable:** M-enabled linestring parquet file regenerated

### Task 3.4: Verify Regenerated Linestring Has M Dimension
- **Agent:** general
- **Action:** Confirm M-dimension exists in regenerated file
- **Steps:**
  1. Run: `duckdb -c "INSTALL spatial; LOAD spatial; SELECT ST_HasM(linestr) as has_m FROM 'pkg/route/testdata/lrs_01001_linestr.parquet'"`
  2. Expected result: `true` (was `false` before regeneration)
  3. Run: `duckdb -c "INSTALL spatial; LOAD spatial; SELECT ST_ZMFlag(linestr) as zm_flag FROM 'pkg/route/testdata/lrs_01001_linestr.parquet'"`
  4. Expected result: `3` (has M but no Z)
  5. Run: `duckdb -c "INSTALL spatial; LOAD spatial; SELECT ST_InterpolatePoint(linestr, ST_Point(602211.736, -2191377.927)) as interpolated_m FROM 'pkg/route/testdata/lrs_01001_linestr.parquet'"`
  6. Verify returns valid M value (not NULL, not error)
- **Deliverable:** Verification report confirming M-dimension exists and ST_InterpolatePoint works

### Task 3.5: Add M-Dimension Test Assertion
- **Agent:** general
- **Action:** Add automated verification to linestring test
- **Steps:**
  1. Modify `pkg/route/route_test.go:230-256` ("linestring table view test")
  2. Add after parquet regeneration (after line 252):
     ```go
     // Verify M-dimension was added to linestring
     hasMResult, err := db.QueryContext(ctx, 
         "SELECT ST_HasM(linestr) FROM read_parquet('testdata/lrs_01001_linestr.parquet')")
     if err != nil {
         t.Fatalf("Failed to check M-dimension: %v", err)
     }
     hasM := hasMResult.Next()
     hasMResult.Close()
     if !hasM {
         t.Fatal("Linestring does not have M dimension after regeneration")
     }
     ```
  3. Run test to verify assertion passes
- **Files to modify:**
  - `pkg/route/route_test.go`
- **Deliverable:** Automated M-dimension verification in test suite

---

## Phase 4: Test Suite Updates

### Task 4.1: Update M-value Test Assertions
- **Agent:** general
- **Action:** Update expected M-values in mvalue tests
- **Steps:**
  1. Review `pkg/mvalue/mvalue_test.go:21-135` (TestCalculatePointsMValue)
  2. Ensure feature flag is enabled in test environment
  3. Run `TestCalculatePointsMValue` with new implementation
  4. Compare actual vs expected M-values (lines 114-119)
     ```go
     expectedMVals := []float64{0, 0.0111}
     ```
  5. Update expected M-values if they differ (ST_InterpolatePoint may have subtle differences in precision)
  6. Verify distance calculations still match (lines 122-128)
  7. Run `TestCalculatePointsMValueBatch` (lines 137-240) similarly
  8. Test with feature flag disabled to ensure fallback works
- **Files to modify:**
  - `pkg/mvalue/mvalue_test.go`
- **Deliverable:** All test assertions updated for both implementations

### Task 4.2: Update Linestring Query Tests
- **Agent:** general
- **Action:** Verify linestring tests work with M-enabled linestrings
- **Steps:**
  1. Review `pkg/route/route_test.go:307-338` ("Initialize LRSRoute from file")
  2. This test loads regenerated parquet file with M-enabled linestring
  3. Verify no failures due to M-dimension
  4. Update any hardcoded expectations if needed
  5. Run test to confirm passes
- **Files to verify/modify:**
  - `pkg/route/route_test.go`
- **Deliverable:** All linestring query tests pass with M-enabled linestrings

### Task 4.3: Update API and Flight Handler Tests
- **Agent:** general
- **Action:** Update API and Flight endpoint tests
- **Steps:**
  1. Review `pkg/api/handler_test.go` (M-value calculation tests)
  2. Run tests with new implementation (feature flag enabled)
  3. Update expected M-values if needed
  4. Review `pkg/flight/server_test.go` (lines 132, 162-164)
  5. Verify Flight operation metadata correct
  6. Test with feature flag disabled to verify fallback
- **Files to modify:**
  - `pkg/api/handler_test.go`
  - `pkg/flight/server_test.go`
- **Deliverable:** API and Flight tests pass with both implementations

### Task 4.4: Add Edge Case Tests
- **Agent:** general
- **Action:** Create comprehensive edge case tests
- **Steps:**
  1. Create new test function `TestCalculatePointsMValueEdgeCases` in `pkg/mvalue/mvalue_test.go`
  2. Test cases:
     - Point exactly on route vertex
     - Point at route start/end
     - Point with maximum distance
     - Route with single segment
     - Route with no M-values (should handle gracefully with NULL M)
     - Null MVAL column in input points
  3. Run both old and new implementations, compare results
  4. Document any differences in behavior
  5. Ensure tolerance thresholds are appropriate (e.g., 0.001 for M-value differences)
- **Files to modify:**
  - `pkg/mvalue/mvalue_test.go` (add new test function)
- **Deliverable:** Edge case tests added and passing for both implementations

---

## Phase 5: Validation & Testing

### Task 5.1: Run Full Test Suite
- **Agent:** general
- **Action:** Execute all tests to verify no regressions
- **Steps:**
  1. Run: `go test ./...` from project root with feature flag enabled
  2. Fix any failing tests
  3. Document any test changes or skipped tests
  4. Run: `go test ./...` with feature flag disabled
  5. Ensure all tests pass with both implementations
  6. Document any tests that differ between implementations
- **Deliverable:** Full test suite passes with both feature flag states

### Task 5.2: Performance Benchmarking
- **Agent:** general
- **Action:** Compare performance of old vs new implementation
- **Steps:**
  1. Create benchmark function `BenchmarkCalculatePointsMValue` in `pkg/mvalue/mvalue_test.go`
  2. Benchmark both old and new implementations:
     ```go
     func BenchmarkCalculatePointsMValueNew(b *testing.B) {
         // Enable feature flag
         os.Setenv("ST_INTERPOLATE_POINT_ENABLED", "true")
         // Run benchmark...
     }
     
     func BenchmarkCalculatePointsMValueOriginal(b *testing.B) {
         // Disable feature flag
         os.Setenv("ST_INTERPOLATE_POINT_ENABLED", "false")
         // Run benchmark...
     }
     ```
  3. Test with varying dataset sizes (100, 1k, 10k points)
  4. Document performance results:
     - Query execution time
     - Memory usage
     - Expected: 60%+ reduction in code complexity, potential performance improvement
  5. Identify any performance regressions or improvements
- **Files to modify:**
  - `pkg/mvalue/mvalue_test.go` (add benchmark function)
- **Deliverable:** Performance comparison report with expected 60%+ complexity reduction

### Task 5.3: Integration Testing
- **Agent:** general
- **Action:** Test with real-world data if available
- **Steps:**
  1. If sample production data exists in `scratch/` directory
  2. Run M-value calculation with both implementations
  3. Compare results:
     - M-value differences within acceptable tolerance (e.g., 0.001)
     - Distance calculations identical
     - All points processed successfully
  4. Check for any edge cases not covered by unit tests
  5. Verify materialized linestrings work with ST_InterpolatePoint
- **Deliverable:** Integration test results confirming correctness with production-like data

---

## Phase 6: Documentation & Cleanup

### Task 6.1: Update Code Documentation
- **Agent:** general
- **Action:** Add/update comments and documentation
- **Steps:**
  1. Update `pkg/mvalue/mvalue.go:15-16` function comment:
     ```go
     // CalculatePointsMValue calculates the M-Value of points relative to an LRS route.
     // Uses DuckDB Spatial's ST_InterpolatePoint function for simplified interpolation.
     // Falls back to complex CTE-based implementation if feature flag disabled.
     // Requires DuckDB Spatial v1.4-andium+ for ST_InterpolatePoint.
     ```
   2. Add comments in `pkg/route/route.go` about WKT construction for M-values (line ~279):
      ```go
      // Construct WKT string "LINESTRING ZM" to create M-enabled linestring
      // Z coordinate set to 0 (placeholder), M values from vertex MVAL column
      // ST_GeomFromText parses WKT to create 3DM linestring for ST_InterpolatePoint support
      ```
  3. Mark original implementation as deprecated (line ~135):
     ```go
     // calculatePointsMValueOriginal implements M-value interpolation using CTEs.
     // DEPRECATED: Kept for backward compatibility via feature flag.
     // Will be removed after migration period.
     ```
   4. Update `pkg/config/feature_flags.go` with usage examples
- **Files to modify:**
  - `pkg/mvalue/mvalue.go`
  - `pkg/route/route.go`
  - `pkg/config/feature_flags.go`
- **Deliverable:** Code comments updated and comprehensive

### Task 6.2: Update README
- **Agent:** general
- **Action:** Document new requirements and changes
- **Steps:**
  1. Check if README.md exists in project root
  2. Add section "DuckDB Spatial Requirements":
     ```markdown
     ## Requirements
     
     ### DuckDB Spatial Extension
     This project requires DuckDB Spatial extension v1.4-andium or later for
     ST_InterpolatePoint function. The extension is automatically loaded during
     runtime via `INSTALL spatial; LOAD spatial;` commands.
     
     ### Feature Flag
     M-value calculation uses ST_InterpolatePoint by default. To use legacy
     implementation:
     
     ```bash
     export ST_INTERPOLATE_POINT_ENABLED=false
     ```
     ```
  3. Add migration notes for existing deployments
  4. Update any performance or architecture descriptions
  5. Document test data regeneration for new installations
- **Files to modify:**
  - `README.md` (if exists)
  - `docs/` directory files (if exists)
- **Deliverable:** README updated with DuckDB Spatial v1.4-andium+ requirement

### Task 6.3: Mark Old Implementation for Deprecation
- **Agent:** general
- **Action:** Add deprecation notices to original implementation
- **Steps:**
  1. In `pkg/mvalue/mvalue.go`, add comments to original implementation:
     ```go
     // calculatePointsMValueOriginal - Original CTE-based implementation
     // 
     // This implementation uses multiple CTEs for:
     // - Finding shortest line to route
     // - Identifying nearest vertex
     // - Manual M-value interpolation
     //
     // DEPRECATED: Use calculatePointsMValueNew() with ST_InterpolatePoint instead.
     // Kept for backward compatibility when feature flag disabled.
     // TODO: Remove in v2.0.0 after migration period.
     ```
  2. Add TODO comment at top of file:
     ```go
     // TODO: Remove calculatePointsMValueOriginal and feature flag after migration period.
     // Target version: v2.0.0
     ```
  3. Do NOT delete old code (feature flag depends on it)
- **Files to modify:**
  - `pkg/mvalue/mvalue.go`
- **Deliverable:** Old implementation marked as deprecated with migration path

---

## Phase 7: Git Branch & Final Checks

### Task 7.1: Create Feature Branch
- **Agent:** general
- **Action:** Create git branch for this feature
- **Steps:**
  1. Ensure clean working directory: `git status`
  2. Create branch: `git checkout -b feature/st-interpolate-point-refactor`
  3. Commit changes with descriptive message:
     ```bash
     git add .
      git commit -m "Refactor: Use ST_InterpolatePoint for M-value calculation

      - Construct WKT strings in LinestringQuery() to create M-enabled linestrings
      - Implement calculatePointsMValueNew() using ST_InterpolatePoint
      - Add feature flag UseSTInterpolatePoint for gradual rollout
      - Keep original implementation as fallback
      - Regenerate test parquet files with M-dimension
      - Update all test assertions
      - Add edge case tests and benchmarks
      ```
  4. Ensure all modified files are committed
  5. Verify branch tracking remote: `git push -u origin feature/st-interpolate-point-refactor`
- **Deliverable:** Feature branch created with all changes

### Task 7.2: Final Validation Checklist
- **Agent:** general
- **Action:** Verify all requirements met
- **Steps:**
  1. [ ] WKT construction used in LRSRoute.LinestringQuery() to create M-enabled linestrings
  2. [ ] ST_InterpolatePoint implemented in calculatePointsMValueNew()
  3. [ ] Feature flag UseSTInterpolatePoint added and working
  4. [ ] Original implementation kept as fallback (marked deprecated)
  5. [ ] Test parquet file lrs_01001_linestr.parquet regenerated with M-dimension
  6. [ ] ST_HasM(linestr) returns true for regenerated file
  7. [ ] All tests pass with feature flag enabled
  8. [ ] All tests pass with feature flag disabled
  9. [ ] Documentation updated (code comments + README)
  10. [ ] Performance benchmarked and documented
  11. [ ] Edge case tests added
  12. [ ] Feature branch created and committed
  13. [ ] Ready for code review
- **Deliverable:** Final validation checklist completed with all items checked

---

## Summary of Files to Modify

### Core Implementation (4 files)
1. `pkg/config/feature_flags.go` - **NEW** - Feature flag infrastructure
2. `pkg/route/route.go` - Modify LinestringQuery() to construct M-enabled linestrings via WKT
3. `pkg/route/batch.go` - No query changes (handles existing materialized files)
4. `pkg/mvalue/mvalue.go` - Add calculatePointsMValueNew(), integrate feature flag, mark original deprecated

### Test Files (5 files)
5. `pkg/mvalue/mvalue_test.go` - Update assertions, add edge case tests, add benchmarks
6. `pkg/route/route_test.go` - Add M-dimension verification after regeneration
7. `pkg/route/batch_test.go` - Verify no regressions
8. `pkg/route/repo_test.go` - Verify materialization tests work
9. `pkg/api/handler_test.go` - Update assertions
10. `pkg/flight/server_test.go` - Update assertions

### Documentation (2 files)
11. `README.md` - Update requirements and migration notes
12. Any existing `docs/*.md` files - Update architecture docs

### Test Data (1 file regenerated)
13. `pkg/route/testdata/lrs_01001_linestr.parquet` - **REGENERATED** with M-dimension

### Files NOT Requiring Changes
- `pkg/route/testdata/lrs_01001_point.parquet` - Already has M values (source data)
- `pkg/route/testdata/lrs_01001_segment.parquet` - Already has M values (derived from point)

---

## Execution Order & Dependencies

```
Phase 1 (Environment & Dependencies)
  ├─ Task 1.1: Verify DuckDB Spatial version
  └─ Task 1.2: Check go.mod compatibility
  ↓
Phase 2 (Core Implementation) ← Blocks on Phase 1
  ├─ Task 2.1: Create feature flag infrastructure
  ├─ Task 2.2: Update LinestringQuery() to create M-enabled linestrings
  ├─ Task 2.3: Implement calculatePointsMValueNew()
  └─ Task 2.4: Integrate feature flag ← Depends on 2.1, 2.3
  ↓
Phase 3 (Test Data Regeneration) ← Blocks on Task 2.2
  ├─ Task 3.1: Identify test parquet files
  ├─ Task 3.2: Verify current linestring schema
  ├─ Task 3.3: Regenerate linestring parquet ← DEPENDS ON Task 2.2
  ├─ Task 3.4: Verify M-dimension exists
  └─ Task 3.5: Add automated M-dimension test
  ↓
Phase 4 (Test Suite Updates) ← Blocks on Phase 2, 3
  ├─ Task 4.1: Update M-value test assertions
  ├─ Task 4.2: Update linestring query tests
  ├─ Task 4.3: Update API and Flight tests
  └─ Task 4.4: Add edge case tests
  ↓
Phase 5 (Validation & Testing) ← Blocks on Phase 4
  ├─ Task 5.1: Run full test suite
  ├─ Task 5.2: Performance benchmarking
  └─ Task 5.3: Integration testing
  ↓
Phase 6 (Documentation & Cleanup) ← Can run in parallel with Phase 5
  ├─ Task 6.1: Update code documentation
  ├─ Task 6.2: Update README
  └─ Task 6.3: Mark old implementation deprecated
  ↓
Phase 7 (Git Branch & Final Checks) ← Blocks on all phases
  ├─ Task 7.1: Create feature branch
  └─ Task 7.2: Final validation checklist
```

---

## Phase 3 Deep Dive: Test Data Regeneration

### Understanding the Current State

**Test Files in `pkg/route/testdata/`:**
- `lrs_01001_point.parquet` (20K) - Raw vertex data with X, Y, M, VERTEX_SEQ
- `lrs_01001_segment.parquet` (49K) - Segment data with M values
- `lrs_01001_linestr.parquet` (11K) - **Linestring geometry (2D, NO M dimension)**

**Current Linestring Parquet Schema:**
```sql
┌─────────────┬─────────────┐
│ column_name │ column_type │
├─────────────┼─────────────┤
│ ROUTEID     │ INTEGER     │
│ linestr     │ BLOB        │  ← 2D geometry (ST_HasM = false)
└─────────────┴─────────────┘
```

**Source Data (`lrs_01001.json`) has M values:**
```json
{
  "hasM": true,
  "paths": [
    [
      [x, y, m],  // Each vertex has 3 values
      [x, y, m],
      ...
    ]
  ]
}
```

### What Changes Before Regeneration?

**LinestringQuery() transformation:**

**Before (2D):**
```sql
SELECT ROUTEID, ST_MakeLine(
  list(ST_Point(lat, lon) ORDER BY vertex_seq)
) as linestr 
FROM view_name
```

**After (M-enabled with WKT construction):**
```sql
SELECT ROUTEID,
  ST_GeomFromText(
    'LINESTRING ZM (' || 
      STRING_AGG(
        CONCAT(lon, ' ', lat, ' 0 ', mval), 
        ', ' 
        ORDER BY vertex_seq ASC
      ) || 
    ')'
  ) as linestr 
FROM view_name
```

**What WKT Construction Does:**
1. Constructs WKT string with format: `"LINESTRING ZM (lon lat 0 mval, lon lat 0 mval, ...)"`
2. Z coordinate set to 0 (placeholder, not used for LRS)
3. M values sourced from vertex MVAL column (preserves per-vertex M values)
4. ST_GeomFromText parses WKT to create 3DM (X, Y, M) linestring
5. Returns a linestring that works correctly with ST_InterpolatePoint

### How Regeneration Happens

**Automated Test in `pkg/route/route_test.go:230-256`:**

```go
t.Run("linestring table view test", func(t *testing.T) {
    lrs, err := NewLRSRouteFromESRIGeoJSON(jsonByte, 0, WKT)
    // ... setup database and arrow ...
    
    // THIS LINE REGENERATES THE PARQUET FILE:
    _, err = db.QueryContext(
        context.Background(), 
        fmt.Sprintf(
            "copy (%s) to 'testdata/lrs_01001_linestr.parquet' (FORMAT parquet);", 
            lrs.LinestringQuery()  // ← Uses new ST_Force3DM implementation
        )
    )
    if err != nil {
        t.Error(err)
    }
})
```

**Regeneration Process:**
1. Load ESRI JSON file (has M values in source data)
2. Call `LinestringQuery()` - NOW uses ST_Force3DM
3. DuckDB executes query → Creates M-enabled linestring geometry
4. Parquet file is **automatically overwritten** with new M-enabled data
5. Test completes with updated file

### Verification After Regeneration

**Step 1: Check M-dimension exists**
```sql
INSTALL spatial; 
LOAD spatial;

SELECT ST_HasM(linestr) as has_m 
FROM 'pkg/route/testdata/lrs_01001_linestr.parquet';
-- Expected: true (was false before)
```

**Step 2: Check ZMFlag**
```sql
SELECT ST_ZMFlag(linestr) as zm_flag 
FROM 'pkg/route/testdata/lrs_01001_linestr.parquet';
-- Expected: 3 (means has M but no Z)
```

**Step 3: Test ST_InterpolatePoint**
```sql
SELECT ST_InterpolatePoint(
  linestr, 
  ST_Point(602211.736, -2191377.927)
) as interpolated_m 
FROM 'pkg/route/testdata/lrs_01001_linestr.parquet';
-- Should return a valid M value (not NULL, not error)
```

### Automated Verification Added to Test

**Add to `pkg/route/route_test.go` after line 252:**

```go
// Verify M-dimension was added to linestring after regeneration
hasMResult, err := db.QueryContext(ctx, 
    "SELECT ST_HasM(linestr) FROM read_parquet('testdata/lrs_01001_linestr.parquet')")
if err != nil {
    t.Fatalf("Failed to check M-dimension: %v", err)
}
hasM := hasMResult.Next()
hasMResult.Close()
if !hasM {
    t.Fatal("Linestring does not have M dimension after regeneration")
}
```

### Key Points About Phase 3

1. **Only 1 file needs regeneration**: `lrs_01001_linestr.parquet`
2. **No manual file manipulation**: Test automatically regenerates when run
3. **Must complete Phase 2.2 first**: LinestringQuery() must use ST_Force3DM
4. **Point and segment files unchanged**: They already have M values from source data
5. **Verification is critical**: Ensure ST_HasM returns true after regeneration
6. **File may increase in size**: M-dimension adds minimal storage overhead

---

## Success Criteria

✅ All tests pass with feature flag enabled  
✅ All tests pass with feature flag disabled (fallback to original)  
✅ Code complexity reduced by ~60% (5 CTEs → 1 simple join)  
✅ Performance equivalent or better than old implementation  
✅ Backward compatible with existing materialized data  
✅ Linestring parquet files regenerated with M-dimension (ST_HasM = true)  
✅ Comprehensive documentation in place  
✅ Feature branch created and ready for code review  

---

## Risk Assessment

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| ST_InterpolatePoint unavailable in production | High | Low | Feature flag fallback to original implementation |
| Performance regression | Medium | Low | Benchmarking before merge, gradual rollout via flag |
| M-enabled linestring breaks existing queries | Medium | Low | Test spatial functions with M-values before deployment |
| Test data incompatibility | Low | Medium | Automated regeneration with verification checks |
| Feature flag edge cases | Low | Low | Test both implementations thoroughly |

---

## Notes for Implementation

### Feature Flag Implementation
- Default: `true` for new installations
- Environment variable: `ST_INTERPOLATE_POINT_ENABLED=true/false`
- Runtime toggle possible via config file
- No breaking changes to function signatures
- Zero architectural changes required

### WKT Construction Approach Details
- **Decision:** Construct WKT string with M-values and parse with `ST_GeomFromText()`
- **Rationale:** 
  - ST_Point only supports 2 parameters (x, y) - no direct M-value support
  - ST_Point4D exists but ST_MakeLine doesn't accept POINT_4D arrays
  - ST_Force3DM requires a single M value for all vertices (incompatible with per-vertex M values)
  - ST_InterpolatePoint REQUIRES linestring to have M-values (fails with 2D linestrings or NULL M-values)
  - WKT `"LINESTRING ZM"` approach works correctly and preserves per-vertex M values
- **Format:** `longitude latitude 0 mvalue` per vertex in WKT
- **Z coordinate:** Set to 0 (placeholder, not used for LRS but required for ZM format)

### DuckDB Spatial Version
- Minimum required: v1.4-andium
- Current duckdb-go: v2.5.0
- Verify via: `SELECT extension_version('spatial')`
- Extension loaded automatically: `INSTALL spatial; LOAD spatial;`

### Test Data Management
- Only linestring parquet files need regeneration
- Point and segment files unchanged (already have M values)
- Regeneration is automated via test execution
- Verification ensures M-dimension exists before proceeding

### Migration Path
1. Deploy with feature flag enabled by default
2. Monitor performance and correctness
3. Gradually enable for specific customers/routes
4. After stable period (e.g., 6 months), deprecate flag
5. Remove original implementation in major version (v2.0.0)
