import json
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq
import sys
import os
import polars as pl

def main():
    # Define server location
    location = "grpc://127.0.0.1:50051"
    
    # Input file path
    input_file = "client/testdata/lambert_15010.parquet"
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)

    # Lambert WKT from pkg/geom/geom.go
    lambert_wkt = 'PROJCS["Indonesia Lambert Conformal Conic",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",115.0],PARAMETER["Standard_Parallel_1",2.0],PARAMETER["Standard_Parallel_2",-7.0],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]'

    try:
        # Create client
        client = flight.FlightClient(location)
        print(f"Connected to Flight server at {location}")

        # Read parquet file
        table = pq.read_table(input_file)
        print(f"Read {table.num_rows} rows from {input_file}")

        # Rename columns to match server expectations
        # TO_STA_LAT -> LAT
        # TO_STA_LONG -> LON
        # Add ROUTEID column (assuming 15010 from filename)
        route_id = "15010"
        
        # Create a new table with required columns
        data = table.to_pandas()
        data['LAT'] = data['TO_STA_LAT']
        data['LON'] = data['TO_STA_LONG']
        data['ROUTEID'] = route_id
        
        # Keep only the columns the server expects or keep all but ensure these exist
        # The server validates ROUTEID, LAT, LON
        final_table = pa.Table.from_pandas(data[['ROUTEID', 'LAT', 'LON']])
        print(f"Prepared table with columns: {final_table.column_names}")

        # Prepare metadata for DoExchange
        action = {
            "operation": "calculate_m_value",
            "crs": lambert_wkt
        }
        descriptor = flight.FlightDescriptor.for_command(json.dumps(action).encode('utf-8'))
        
        # Start DoExchange
        options = flight.FlightCallOptions()
        writer, reader = client.do_exchange(descriptor, options=options)

        # In PyArrow, we can write a batch with app_metadata if needed, 
        # but here we use the descriptor for the command.
        
        # Construct a RecordBatch from the table.
        batches = final_table.to_batches()
        
        # Begin the stream
        writer.begin(final_table.schema)
        
        # Write data
        print("Sending data to server...")
        for batch in batches:
            writer.write(batch)
            
        # Half-close the stream (send side)
        writer.done_writing()
        print("Closed client write-side. Reading results...")

        # Read results
        batches_in = []
        try:
            # We use an iterator over the reader to get FlightStreamChunk objects
            for chunk in reader:
                if chunk.data is not None:
                    print(f"Received batch with {chunk.data.num_rows} rows")
                    batches_in.append(chunk.data)
                elif chunk.app_metadata is not None:
                    print(f"Received metadata: {chunk.app_metadata.to_pybytes()}")
        except Exception as e:
            print(f"Error during reading results: {e}")
            if not batches_in:
                raise e

        if not batches_in:
            print("No batches received from server.")
            return

        result_table = pa.Table.from_batches(batches_in)
        print(f"Received total results: {result_table.num_rows} rows")
        
        # Print specific columns
        df = result_table.to_pandas()
        if 'MVAL' in df.columns:
            print("\nResults (MVAL, dist_to_line):")
            print(df[['MVAL', 'dist_to_line']].head())
        else:
            print("\nResults columns:", df.columns)
            print(df.head())

    except Exception as e:
        print(f"\nAn error occurred: {type(e).__name__}: {e}")
        if hasattr(e, 'extra_info'):
            print(f"Extra info: {e.extra_info}")
        # Print traceback for debugging
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
