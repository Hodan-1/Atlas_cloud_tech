import subprocess
import time

# Start the monitor.py script as a background process
monitor_process = subprocess.Popen(["python", "monitor.py"])

# Your main analysis code goes here
print("Starting the main analysis...")
start_time = time.time()
# Simulate a CPU-intensive task
for i in range(10000000):
    pass  # Replace this with your actual analysis code
end_time = time.time()
print(f"Analysis completed in {end_time - start_time:.2f} seconds")

# Optionally, wait for the monitor.py process to finish
monitor_process.wait()

print("Analysis and monitoring complete.")
