import json
import pandas as pd
import matplotlib.pyplot as plt


def load_and_parse_data(filepath):
  """Parses the raw JSON file and extracts the embedded SSE metrics."""
  with open(filepath, 'r') as f:
    raw_data = json.load(f)

  parsed_records = []

  for entry in raw_data:
    # Extract the string from the "message" key
    msg = entry.get('message', '').strip()

    # Check if it's formatted as Server Sent Events (SSE) data
    if msg.startswith('data: '):
      # Strip the "data: " prefix and parse the inner JSON
      json_str = msg[6:]
      try:
        payload = json.loads(json_str)
        # Add the outer client-side timestamp just in case we need it
        payload['client_timestamp_ms'] = entry.get('timestamp')
        parsed_records.append(payload)
      except json.JSONDecodeError:
        print(f"Skipping malformed JSON string: {json_str}")
        continue

  return pd.DataFrame(parsed_records)


def process_metrics(df):
  """Transforms raw metrics into usable performance statistics."""
  # Convert cumulative execution time from nanoseconds to seconds
  df['execution_time_sec'] = df['execution_time'] / 1e9

  # Calculate the time taken for *just this batch* by calculating the difference
  # from the previous row's execution time. Fill the first row with its own value.
  df['batch_duration_sec'] = df['execution_time_sec'].diff().fillna(df['execution_time_sec'].iloc[0])

  # Calculate throughput (items per second) for each batch
  df['throughput_ips'] = df['processed_in_batch'] / df['batch_duration_sec']

  return df


def print_statistics(df):
  """Prints a summary of statistical outputs."""
  print("=== Performance Statistics ===")
  print(f"Total Batches Processed:  {df['batch'].max()}")
  print(f"Total Items Ingested:     {df['total_ingested'].max():,}")
  print(f"Total Execution Time:     {df['execution_time_sec'].max():.2f} seconds")
  print(f"Overall Average Speed:    {df['total_ingested'].max() / df['execution_time_sec'].max():.2f} items/sec")
  print("-" * 30)
  print(f"Average Batch Duration:   {df['batch_duration_sec'].mean():.4f} seconds")
  print(
    f"Max Batch Duration:       {df['batch_duration_sec'].max():.4f} seconds (Batch {df.loc[df['batch_duration_sec'].idxmax(), 'batch']})")
  print(
    f"Min Batch Duration:       {df['batch_duration_sec'].min():.4f} seconds (Batch {df.loc[df['batch_duration_sec'].idxmin(), 'batch']})")
  print("-" * 30)
  print(f"Peak Throughput:          {df['throughput_ips'].max():.2f} items/sec")
  print(f"Lowest Throughput:        {df['throughput_ips'].min():.2f} items/sec")
  print("==============================\n")


def plot_performance(df):
  """Generates a 3-panel dashboard of performance metrics."""
  # Create a figure with 3 subplots stacked vertically
  fig, axes = plt.subplots(3, 1, figsize=(12, 14), sharex=True)
  fig.suptitle('Data Ingestion Performance Dashboard', fontsize=16)

  # 1. Cumulative Ingestion
  axes[0].plot(df['batch'], df['total_ingested'], marker='o', color='blue', linestyle='-')
  axes[0].set_title('Cumulative Items Ingested over Time')
  axes[0].set_ylabel('Total Items')
  axes[0].grid(True, alpha=0.3)

  # 2. Batch Processing Duration
  axes[1].bar(df['batch'], df['batch_duration_sec'], color='orange', alpha=0.7)
  axes[1].plot(df['batch'], df['batch_duration_sec'], marker='x', color='red', linestyle=':')
  axes[1].set_title('Processing Duration per Batch (Latency)')
  axes[1].set_ylabel('Seconds')
  axes[1].grid(True, alpha=0.3)

  # 3. Throughput per Batch
  axes[2].plot(df['batch'], df['throughput_ips'], marker='s', color='green', linestyle='-')
  axes[2].set_title('Ingestion Throughput per Batch')
  axes[2].set_xlabel('Batch Number')
  axes[2].set_ylabel('Items / Second')
  axes[2].grid(True, alpha=0.3)

  plt.tight_layout()
  plt.show()


if __name__ == "__main__":
  # Assume your raw data is saved in a file called 'ingest_logs.json'
  # df_raw = load_and_parse_data('ingest_logs.json')

  # For demonstration, let's use the mocked logic with your provided snippet:
  sample_data = [
    {"timestamp": 1787151301553,
     "message": "data: {\"status\": \"processing\", \"batch\": 1, \"processed_in_batch\": 1000, \"total_ingested\": 1000, \"execution_time\": 7616948907}\n\n"},
    {"timestamp": 1787151301785,
     "message": "data: {\"status\": \"processing\", \"batch\": 2, \"processed_in_batch\": 1000, \"total_ingested\": 2000, \"execution_time\": 7855006801}\n\n"},
    {"timestamp": 1787151301910,
     "message": "data: {\"status\": \"processing\", \"batch\": 3, \"processed_in_batch\": 1000, \"total_ingested\": 3000, \"execution_time\": 7979492643}\n\n"},
    {"timestamp": 1787151302074,
     "message": "data: {\"status\": \"processing\", \"batch\": 4, \"processed_in_batch\": 1000, \"total_ingested\": 4000, \"execution_time\": 8126754896}\n\n"}
  ]

  # Simulating the parsing logic
  parsed = [json.loads(x['message'].strip()[6:]) for x in sample_data]
  df = pd.DataFrame(parsed)

  # Process and visualize
  df_processed = process_metrics(df)
  print_statistics(df_processed)
  plot_performance(df_processed)