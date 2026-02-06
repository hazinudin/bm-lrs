package route

import (
	"fmt"
	"strings"
)

type sourceFile struct {
	filePath     string
	routes       []string
	materialized bool
}
type batchSourceFiles struct {
	Point      []sourceFile
	Segment    []sourceFile
	LineString []sourceFile
}

type LRSRouteBatch struct {
	routes       map[string]LRSRoute
	sourceFiles  *batchSourceFiles
	latitudeCol  string
	longitudeCol string
	mValueCol    string
}

// Add LRSRoute to the batch
func (l *LRSRouteBatch) AddRoute(route LRSRoute) error {
	if l.routes == nil {
		l.routes = make(map[string]LRSRoute)
	}

	if l.sourceFiles == nil {
		l.sourceFiles = &batchSourceFiles{}
		// Initialize columns from the first route
		l.latitudeCol = route.LatitudeColumn()
		l.longitudeCol = route.LongitudeColumn()
		l.mValueCol = route.MValueColumn()
	}

	// Check if the route is materialized or not.
	if !route.IsMaterialized() {
		err := route.Sink()
		if err != nil {
			return fmt.Errorf("failed to sink point RecordBatch: %v", err)
		}
	}

	// Check if Point file exists in sourceFiles, if not add it
	pointFile := route.GetPointFile()

	if pointFile != nil {
		for i := range l.sourceFiles.Point {
			if l.sourceFiles.Point[i].filePath == *pointFile {
				l.sourceFiles.Point[i].routes = append(l.sourceFiles.Point[i].routes, route.GetRouteID())
				goto SkipPoint
			}
		}

		if !route.push_down {
			l.sourceFiles.Point = append(l.sourceFiles.Point, sourceFile{
				filePath:     *pointFile,
				routes:       []string{},
				materialized: true,
			})
		} else {
			l.sourceFiles.Point = append(l.sourceFiles.Point, sourceFile{
				filePath:     *pointFile,
				routes:       []string{route.GetRouteID()},
				materialized: true,
			})
		}
	}

SkipPoint:

	// Check if Segment file exists in sourceFiles, if not add it
	segmentFile := route.GetSegmentFile()

	if segmentFile != nil {
		for i := range l.sourceFiles.Segment {
			if l.sourceFiles.Segment[i].filePath == *segmentFile {
				l.sourceFiles.Segment[i].routes = append(l.sourceFiles.Segment[i].routes, route.GetRouteID())
				goto SkipSegment
			}
		}

		if !route.push_down {
			l.sourceFiles.Segment = append(l.sourceFiles.Segment, sourceFile{
				filePath:     *segmentFile,
				routes:       []string{},
				materialized: true,
			})
		} else {
			l.sourceFiles.Segment = append(l.sourceFiles.Segment, sourceFile{
				filePath:     *segmentFile,
				routes:       []string{route.GetRouteID()},
				materialized: true,
			})
		}
	} else {
		l.sourceFiles.Segment = append(l.sourceFiles.Segment, sourceFile{
			filePath:     route.SegmentQuery(),
			routes:       []string{},
			materialized: false,
		})
	}

SkipSegment:

	// Check if Linestring file exists in sourceFiles, if not add it
	linestrFile := route.GetLineFile()

	if linestrFile != nil {
		for i := range l.sourceFiles.LineString {
			if l.sourceFiles.LineString[i].filePath == *linestrFile {
				l.sourceFiles.LineString[i].routes = append(l.sourceFiles.LineString[i].routes, route.GetRouteID())
				goto SkipLinestr
			}
		}

		if !route.push_down {
			l.sourceFiles.LineString = append(l.sourceFiles.LineString, sourceFile{
				filePath: *linestrFile,
				routes:   []string{},
			})
		} else {
			l.sourceFiles.LineString = append(l.sourceFiles.LineString, sourceFile{
				filePath: *linestrFile,
				routes:   []string{route.GetRouteID()},
			})
		}
	} else {
		l.sourceFiles.LineString = append(l.sourceFiles.LineString, sourceFile{
			filePath:     route.LinestringQuery(),
			routes:       []string{},
			materialized: false,
		})
	}

SkipLinestr:

	// Finally add route to routes map
	l.routes[route.GetRouteID()] = route

	return nil
}

// Release all temporary files or RecordBatches
func (l *LRSRouteBatch) Release() {
	for _, route := range l.routes {
		route.Release()
	}
}

// ViewName returns a query for loading point data from all source files in the batch
func (l *LRSRouteBatch) ViewName() string {
	if l.sourceFiles == nil || len(l.sourceFiles.Point) == 0 {
		return ""
	}

	var queries []string
	var noPushDownFiles []string
	for _, sf := range l.sourceFiles.Point {
		if sf.materialized {
			if len(sf.routes) == 0 {
				noPushDownFiles = append(noPushDownFiles, sf.filePath)
			} else {
				routeList := strings.Join(sf.routes, "','")
				queries = append(queries, fmt.Sprintf(`SELECT * FROM "%s" WHERE ROUTEID IN ['%s']`, sf.filePath, routeList))
			}
		} else {
			queries = append(queries, sf.filePath)
		}
	}

	if len(noPushDownFiles) > 0 {
		noPushDownQuery := fmt.Sprintf(`SELECT * FROM read_parquet(["%s"])`, strings.Join(noPushDownFiles, `", "`))
		queries = append(queries, noPushDownQuery)
	}

	return fmt.Sprintf("(%s)", strings.Join(queries, " UNION ALL "))
}

// SegmentQuery returns a query for loading segment data from all source files in the batch
func (l *LRSRouteBatch) SegmentQuery() string {
	if l.sourceFiles == nil || len(l.sourceFiles.Segment) == 0 {
		return ""
	}

	var queries []string
	var noPushDownFiles []string
	for _, sf := range l.sourceFiles.Segment {
		if sf.materialized {
			if len(sf.routes) == 0 {
				noPushDownFiles = append(noPushDownFiles, sf.filePath)
			} else {
				routeList := strings.Join(sf.routes, "','")
				queries = append(queries, fmt.Sprintf(`SELECT * FROM "%s" WHERE ROUTEID IN ['%s']`, sf.filePath, routeList))
			}
		} else {
			queries = append(queries, sf.filePath)
		}
	}

	if len(noPushDownFiles) > 0 {
		noPushDownQuery := fmt.Sprintf(`SELECT * FROM read_parquet([%s])`, strings.Join(noPushDownFiles, ", "))
		queries = append(queries, noPushDownQuery)
	}

	return strings.Join(queries, " UNION ALL ")
}

// LinestringQuery returns a query for loading linestring data from all source files in the batch
func (l *LRSRouteBatch) LinestringQuery() string {
	if l.sourceFiles == nil || len(l.sourceFiles.LineString) == 0 {
		return ""
	}

	var queries []string
	var noPushDownFiles []string
	for _, sf := range l.sourceFiles.LineString {
		if sf.materialized {
			if len(sf.routes) == 0 {
				noPushDownFiles = append(noPushDownFiles, sf.filePath)
			} else {
				routeList := strings.Join(sf.routes, "','")
				queries = append(queries, fmt.Sprintf(`SELECT * FROM "%s" WHERE ROUTEID IN ['%s']`, sf.filePath, routeList))
			}
		} else {
			queries = append(queries, sf.filePath)
		}
	}

	if len(noPushDownFiles) > 0 {
		noPushDownQuery := fmt.Sprintf(`SELECT * FROM read_parquet([%s])`, strings.Join(noPushDownFiles, ", "))
		queries = append(queries, noPushDownQuery)
	}

	return strings.Join(queries, " UNION ALL ")
}

func (l *LRSRouteBatch) LatitudeColumn() string {
	return l.latitudeCol
}

func (l *LRSRouteBatch) LongitudeColumn() string {
	return l.longitudeCol
}

func (l *LRSRouteBatch) MValueColumn() string {
	return l.mValueCol
}
