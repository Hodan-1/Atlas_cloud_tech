# benchmark.py
import time
import subprocess
import os
import sys

# Determine which environment we're running in
deployment_type = os.environ.get('DEPLOYMENT_TYPE', 'unknown')

# Start the CPU monitor
monitor_process = subprocess.Popen(["python", "cpu_monitor.py", deployment_type])

print(f"Starting benchmark in {deployment_type} environment...")
start_time = time.time()

# Run  actual workload here - this should be identical in both environments
# For example, process a specific dataset or run a fixed number of calculations
# Replace this with your actual workload:
for i in range(10000000):
    _ = i * i * i  # CPU-intensive calculation

end_time = time.time()
print(f"Benchmark completed in {end_time - start_time:.2f} seconds")

# Wait for the monitor to finish
monitor_process.wait()
print("Benchmark and monitoring complete.")
