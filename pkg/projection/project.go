package projection

import (
	"bm-lrs/pkg/geom"
	"bm-lrs/pkg/route"
	"bytes"
	"context"
	"database/sql"
	"text/template"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/duckdb/duckdb-go/v2"
)

// Transform geometry object to a target CRS
func Transform(obj geom.Geometry, crs string, inverted bool) (geom.Geometry, error) {
	c, err := duckdb.NewConnector("", nil)

	if err != nil {
		return nil, err
	}

	defer c.Close()

	conn, err := c.Connect(context.Background())

	if err != nil {
		return nil, err
	}

	defer conn.Close()

	ar, err := duckdb.NewArrowFromConn(conn)

	if err != nil {
		return nil, err
	}

	records := obj.GetRecords()
	rr, err := array.NewRecordReader(records[0].Schema(), records)

	if err != nil {
		return nil, err
	}

	release, err := ar.RegisterView(rr, "records")
	defer release()

	// Open database connection
	db := sql.OpenDB(c)
	db.ExecContext(context.Background(), "install spatial; load spatial;")

	// Create geometry colum
	var cte_query string
	if inverted {
		cte_query = `
		with 
		transformed as (
		select 
		* exclude({{.LatCol}}, {{.LongCol}}), 
		ST_Transform(ST_Point({{.LongCol}}, {{.LatCol}}), '{{.OriginCRS}}', '{{.TargetCRS}}') as shape 
		from records
		) 
		`
	} else {
		cte_query = `
		with 
		transformed as (
		select 
		* exclude({{.LatCol}}, {{.LongCol}}), 
		ST_Transform(ST_Point({{.LatCol}}, {{.LongCol}}), '{{.OriginCRS}}', '{{.TargetCRS}}') as shape 
		from records
		) 
		`
	}

	query := cte_query + `
	-- select from transformed
	select * exclude(shape), ST_X(shape) as {{.LongCol}}, ST_Y(shape) as {{.LatCol}} from transformed
	`

	data := map[string]string{
		"LatCol":    "LAT",
		"LongCol":   "LONG",
		"OriginCRS": obj.GetCRS(),
		"TargetCRS": crs,
	}

	template, err := template.New("queryTemplate").Parse(query)

	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = template.Execute(&buf, data)

	if err != nil {
		return nil, err
	}

	out_reader, err := ar.QueryContext(
		context.Background(),
		buf.String(),
	)

	if err != nil {
		return nil, err
	}

	defer out_reader.Release()

	// Fetch the RecordBatch reader
	var recs []arrow.RecordBatch

	for out_reader.Next() {
		rec := out_reader.RecordBatch()
		rec.Retain()

		recs = append(recs, rec)

		// num_rows := int(rec.NumRows())
		// num_cols := int(rec.NumCols())

		// for i := range num_rows {
		// 	for colIdx := range num_cols {
		// 		col := rec.Column(colIdx)

		// 		fmt.Printf("col %s: %s", rec.ColumnName(colIdx), col.ValueStr(i))
		// 	}
		// }

		switch obj.GetGeometryType() {

		case geom.LRS:
			out := route.NewLRSRoute(
				obj.GetAttributes()["RouteID"].(string),
				recs,
				crs,
			)

			return &out, nil

		case geom.POINTS:
			out := geom.NewPoints(
				recs,
				crs,
			)

			return &out, nil
		}
	}

	return nil, nil
}
