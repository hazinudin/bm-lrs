import json
import os
import sys
import time

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq


def main():
    # Define server location
    location = "grpc://127.0.0.1:50051"

    # Input file path
    input_file = "scratch/rni_2_2025.parquet"
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)

    # Output file path
    output_file = "scratch/rni_2_2025_results.parquet"

    # Chunk size
    chunk_size = 10000

    try:
        # Create client
        client = flight.FlightClient(location)
        print(f"Connected to Flight server at {location}")

        # Read the full table once
        full_table = pq.read_table(input_file)
        total_rows = full_table.num_rows
        print(f"Total rows in {input_file}: {total_rows}")

        # Prepare metadata for DoExchange
        action = {"operation": "calculate_m_value", "crs": 'EPSG:4326'}
        descriptor = flight.FlightDescriptor.for_command(
            json.dumps(action).encode("utf-8")
        )

        # Start DoExchange - single connection for all chunks
        print("\nStarting DoExchange connection...")
        options = flight.FlightCallOptions()
        writer, reader = client.do_exchange(descriptor, options=options)

        # Define a consistent schema for all chunks
        # This ensures all chunks use the same schema regardless of decimal precision differences
        input_schema = pa.schema([
            pa.field("ROUTEID", pa.string()),
            pa.field("LAT", pa.float64()),
            pa.field("LON", pa.float64()),
        ])

        # Begin the stream with the input schema
        writer.begin(input_schema)
        print(f"Initialized stream with input schema: {input_schema}")

        # Send all chunks first
        print(f"Sending all {total_rows} rows in chunks of {chunk_size}...")
        rows_sent = 0

        # Track send start time
        send_start_time = time.time()
        print(f"SEND_START: {send_start_time}")

        for offset in range(0, total_rows, chunk_size):
            chunk_end = min(offset + chunk_size, total_rows)

            # Slice the full table to get the chunk
            chunk_table = full_table.slice(offset, chunk_size)

            # Convert to pandas for column manipulation
            data = chunk_table.to_pandas()

            # Extract LINKID as ROUTEID (split at first underscore if needed)
            # For RNI data, LINKID is used as route identifier
            data["ROUTEID"] = data["LINKID"].astype(str)

            # # Rename columns to match server expectations
            # # Cast to float to ensure consistent precision across chunks
            data["LAT"] = data["TO_STA_LAT"].astype(float)
            data["LON"] = data["TO_STA_LONG"].astype(float)

            # # Keep only the columns the server expects: ROUTEID, LAT, LON
            # # This ensures the server returns a consistent schema
            data = data[["ROUTEID", "LAT", "LON"]]

            # Create table with only the required columns
            final_table = pa.Table.from_pandas(data)

            # Cast to the input schema to ensure consistency
            final_table = final_table.cast(input_schema)

            # Write data
            batches = final_table.to_batches()
            for batch in batches:
                writer.write(batch)

            rows_sent += final_table.num_rows
            print(f"Sent chunk {offset}-{chunk_end} ({final_table.num_rows} rows). Total sent: {rows_sent}")

        # Half-close the stream (send side) - all chunks sent
        print("\nAll chunks sent. Closing write-side...")
        writer.done_writing()

        # Track send end time
        send_end_time = time.time()
        print(f"SEND_END: {send_end_time}")
        print(f"SEND_DURATION: {send_end_time - send_start_time:.3f}s")

        print("Write-side closed. Reading results...")

        # Now read all results
        result_writer = None
        result_schema = None
        total_rows_received = 0

        try:
            # Track receive start time (first chunk)
            receive_start_time = None

            for chunk in reader:
                if chunk.data is not None:
                    # Track receive start time on first chunk
                    if receive_start_time is None:
                        receive_start_time = time.time()
                        print(f"RECEIVE_START: {receive_start_time}")

                    # print(f"Received batch with {chunk.data.num_rows} rows")

                    # If this is the first batch, initialize the parquet writer
                    if result_writer is None:
                        # Use the schema from the server as-is
                        result_schema = chunk.data.schema
                        result_writer = pq.ParquetWriter(output_file, result_schema)
                        print(f"Created parquet writer with schema: {result_schema}")

                    # Write the batch directly without casting
                    result_writer.write_table(pa.Table.from_batches([chunk.data]))
                    total_rows_received += chunk.data.num_rows
                elif chunk.app_metadata is not None:
                    print(f"Received metadata: {chunk.app_metadata.to_pybytes()}")

            # Track receive end time after all chunks received
            receive_end_time = time.time()
            print(f"RECEIVE_END: {receive_end_time}")
            if receive_start_time is not None:
                print(f"RECEIVE_DURATION: {receive_end_time - receive_start_time:.3f}s")
        except Exception as e:
            print(f"Error during reading results: {e}")
            import traceback
            traceback.print_exc()
            writer.close()
            raise e

        # Close writer
        writer.close()

        # Close the parquet writer if it was initialized
        if result_writer is not None:
            result_writer.close()
            print(f"\nSuccessfully wrote {total_rows_received} rows to {output_file}")

            # Total duration from send start to receive end
            if receive_start_time is not None:
                total_duration = receive_end_time - send_start_time
                print(f"TOTAL_DURATION: {total_duration:.3f}s")
        else:
            print("\nNo batches received from server.")
            return

    except Exception as e:
        print(f"\nAn error occurred: {type(e).__name__}: {e}")
        if hasattr(e, "extra_info"):
            print(f"Extra info: {e.extra_info}")
        # Print traceback for debugging
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
