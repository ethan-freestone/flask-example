import json
import matplotlib.pyplot as plt
import pandas as pd


def load_and_parse_data(filepath):
  """Parses the raw JSON log file, extracting incoming SSE messages and

  handling both processing batches and the final completion event.
  """
  with open(filepath, 'r') as f:
    raw_data = json.load(f)

  parsed_records = []

  for entry in raw_data:
    # Only look at incoming network messages
    if entry.get('type') != 'incoming':
      continue

    msg = entry.get('message', '').strip()

    if msg.startswith('data: '):
      json_str = msg[6:]
      try:
        payload = json.loads(json_str)
        # Capture outer log metadata
        payload['log_timestamp'] = entry.get('timestamp')
        payload['seq'] = entry.get('seq')
        parsed_records.append(payload)
      except json.JSONDecodeError:
        print(f'Skipping malformed JSON string: {json_str}')
        continue

  return pd.DataFrame(parsed_records)


def process_metrics(df):
  """Transforms raw metrics into usable performance statistics,

  safely handling the final 'complete' status event.
  """
  # Separate processing batches from the final completion event
  df_proc = df[df['status'] == 'processing'].copy()
  df_comp = df[df['status'] == 'complete'].copy()

  # Convert cumulative execution time from nanoseconds to seconds for batches
  df_proc['execution_time_sec'] = df_proc['execution_time'] / 1e9

  # Calculate batch duration and throughput
  df_proc['batch_duration_sec'] = df_proc['execution_time_sec'].diff().fillna(
      df_proc['execution_time_sec'].iloc[0]
  )
  df_proc['throughput_ips'] = (
      df_proc['processed_in_batch'] / df_proc['batch_duration_sec']
  )

  # If a completion record exists, convert its execution time as well
  if not df_comp.empty:
    df_comp['execution_time_sec'] = df_comp['execution_time'] / 1e9

  return df_proc, df_comp


def print_statistics(df_proc, df_comp):
  """Prints a summary of statistical outputs using both batch and final data."""
  print('=== Performance Statistics ===')
  print(f"Total Batches Processed:  {df_proc['batch'].max()}")

  # Use the final 'complete' record for authoritative total ingested & time if available
  if not df_comp.empty:
    total_items = df_comp['total_ingested'].max()
    total_time = df_comp['execution_time_sec'].max()
  else:
    total_items = df_proc['total_ingested'].max()
    total_time = df_proc['execution_time_sec'].max()

  print(f'Total Items Ingested:     {total_items:,}')
  print(f'Total Execution Time:     {total_time:.2f} seconds')
  print(f'Overall Average Speed:    {total_items / total_time:.2f} items/sec')
  print('-' * 30)
  print(f"Average Batch Duration:   {df_proc['batch_duration_sec'].mean():.4f} seconds")
  print(
      f"Max Batch Duration:       {df_proc['batch_duration_sec'].max():.4f}"
      f" seconds (Batch"
      f" {df_proc.loc[df_proc['batch_duration_sec'].idxmax(), 'batch']})"
  )
  print(
      f"Min Batch Duration:       {df_proc['batch_duration_sec'].min():.4f}"
      f" seconds (Batch"
      f" {df_proc.loc[df_proc['batch_duration_sec'].idxmin(), 'batch']})"
  )
  print('-' * 30)
  print(f"Peak Throughput:          {df_proc['throughput_ips'].max():.2f} items/sec")
  print(f"Lowest Throughput:        {df_proc['throughput_ips'].min():.2f} items/sec")
  print('==============================\n')


def plot_performance(df_proc, df_comp):
  """Generates a 3-panel dashboard incorporating final completion points."""
  fig, axes = plt.subplots(3, 1, figsize=(12, 14), sharex=True)
  fig.suptitle('Data Ingestion Performance Dashboard', fontsize=16)

  # 1. Cumulative Ingestion
  axes[0].plot(
      df_proc['batch'],
      df_proc['total_ingested'],
      marker='o',
      color='blue',
      linestyle='-',
      label='Batch Progress',
  )
  if not df_comp.empty:
    # Plot final authoritative completion point if batch count can be estimated or mapped
    final_batch = df_proc['batch'].max() + 1
    axes[0].scatter(
        [final_batch],
        df_comp['total_ingested'],
        color='purple',
        s=100,
        zorder=5,
        label='Complete Marker',
    )
  axes[0].set_title('Cumulative Items Ingested over Time')
  axes[0].set_ylabel('Total Items')
  axes[0].grid(True, alpha=0.3)
  axes[0].legend()

  # 2. Batch Processing Duration
  axes[1].bar(
      df_proc['batch'],
      df_proc['batch_duration_sec'],
      color='orange',
      alpha=0.7,
  )
  axes[1].plot(
      df_proc['batch'],
      df_proc['batch_duration_sec'],
      marker='x',
      color='red',
      linestyle=':',
  )
  axes[1].set_title('Processing Duration per Batch (Latency)')
  axes[1].set_ylabel('Seconds')
  axes[1].grid(True, alpha=0.3)

  # 3. Throughput per Batch
  axes[2].plot(
      df_proc['batch'],
      df_proc['throughput_ips'],
      marker='s',
      color='green',
      linestyle='-',
  )
  axes[2].set_title('Ingestion Throughput per Batch')
  axes[2].set_xlabel('Batch Number')
  axes[2].set_ylabel('Items / Second')
  axes[2].grid(True, alpha=0.3)

  plt.tight_layout()
  plt.show()


if __name__ == '__main__':
  # Replace with your actual log file path
  #df = load_and_parse_data('rx-harvest.json')
  df = load_and_parse_data('generator-harvest.json')

  # Process, print stats, and plot
  df_proc, df_comp = process_metrics(df)
  print_statistics(df_proc, df_comp)
  plot_performance(df_proc, df_comp)