import psutil
import time
import matplotlib.pyplot as plt

cpu_usage_history = []
timestamp_history = []

start_time = time.time()
duration = 60  # Monitor for 60 seconds
interval = 1  # Sample every 1 second

while time.time() - start_time < duration:
    cpu_percent = psutil.cpu_percent(interval=interval)
    cpu_usage_history.append(cpu_percent)
    timestamp_history.append(time.time() - start_time)
    time.sleep(interval)

# Create the plot
plt.figure(figsize=(10, 6))
plt.plot(timestamp_history, cpu_usage_history)
plt.xlabel("Time (seconds)")
plt.ylabel("CPU Usage (%)")
plt.title("CPU Usage Over Time")
plt.grid(True)

# Save the plot to the /app_data directory
plt.savefig("/app_data/cpu_usage.png")
print("CPU usage graph saved to /app_data/cpu_usage.png")
