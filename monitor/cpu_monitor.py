# cpu_monitor.py
import psutil
import time
import matplotlib.pyplot as plt
import sys
import os

# Get the deployment type from command line argument
deployment_type = sys.argv[1] if len(sys.argv) > 1 else "unknown"

cpu_usage_history = []
timestamp_history = []
start_time = time.time()
duration = 60  # Monitor for 60 seconds
interval = 1   # Sample every 1 second

print(f"Starting CPU monitoring for {deployment_type} environment...")

while time.time() - start_time < duration:
    cpu_percent = psutil.cpu_percent(interval=interval)
    cpu_usage_history.append(cpu_percent)
    timestamp_history.append(time.time() - start_time)
    print(f"Time: {timestamp_history[-1]:.1f}s, CPU: {cpu_percent:.1f}%")

# Create the plot
plt.figure(figsize=(10, 6))
plt.plot(timestamp_history, cpu_usage_history)
plt.xlabel("Time (seconds)")
plt.ylabel("CPU Usage (%)")
plt.title(f"CPU Usage Over Time - {deployment_type}")
plt.grid(True)

# Create output directory if it doesn't exist
os.makedirs("/app_data", exist_ok=True)

# Save the plot with the deployment type in the filename
output_file = f"/app_data/cpu_usage_{deployment_type}.png"
plt.savefig(output_file)

# Also save the raw data for later analysis
import csv
data_file = f"/app_data/cpu_data_{deployment_type}.csv"
with open(data_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["Time (s)", "CPU (%)"])
    for t, c in zip(timestamp_history, cpu_usage_history):
        writer.writerow([t, c])

print(f"CPU usage graph saved to {output_file}")
print(f"CPU data saved to {data_file}")
