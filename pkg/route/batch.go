package route

type sourceFile struct {
	filePath string
	routes   []string
}
type batchSourceFiles struct {
	Point      []sourceFile
	Segment    []sourceFile
	LineString []sourceFile
}

type LRSRouteBatch struct {
	routes      map[string]LRSRoute
	sourceFiles *batchSourceFiles
}

// Add LRSRoute to the batch
func (l *LRSRouteBatch) AddRoute(route LRSRoute) {
	if l.routes == nil {
		l.routes = make(map[string]LRSRoute)
	}

	// Check if the route is materialized or not.
	if !route.IsMaterialized() {
		route.Sink()
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

		l.sourceFiles.Point = append(l.sourceFiles.Point, sourceFile{
			filePath: *pointFile,
			routes:   []string{route.GetRouteID()},
		})
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

		l.sourceFiles.Segment = append(l.sourceFiles.Segment, sourceFile{
			filePath: *segmentFile,
			routes:   []string{route.GetRouteID()},
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

		l.sourceFiles.LineString = append(l.sourceFiles.LineString, sourceFile{
			filePath: *linestrFile,
			routes:   []string{route.GetRouteID()},
		})
	}

SkipLinestr:

	// Finally add route to routes map
	l.routes[route.GetRouteID()] = route
}

// Release all temporary files or RecordBatches
func (l *LRSRouteBatch) Release() {
	for _, route := range l.routes {
		route.Release()
	}
}
