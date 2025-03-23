# compare_results.py
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load the data
docker_data = pd.read_csv("output/cpu_data_docker-compose.csv")
k8s_data = pd.read_csv("output/cpu_data_kubernetes.csv")

# Create comparison plot
plt.figure(figsize=(12, 8))
plt.plot(docker_data["Time (s)"], docker_data["CPU (%)"], label="Docker Compose")
plt.plot(k8s_data["Time (s)"], k8s_data["CPU (%)"], label="Kubernetes")
plt.xlabel("Time (seconds)")
plt.ylabel("CPU Usage (%)")
plt.title("CPU Usage Comparison: Docker Compose vs Kubernetes")
plt.legend()
plt.grid(True)
plt.savefig("output/cpu_comparison.png")

# Calculate statistics
docker_avg = docker_data["CPU (%)"].mean()
k8s_avg = k8s_data["CPU (%)"].mean()
docker_max = docker_data["CPU (%)"].max()
k8s_max = k8s_data["CPU (%)"].max()

# Print comparison
print("\nCPU Usage Comparison:")
print(f"{'Metric':<20} {'Docker Compose':<15} {'Kubernetes':<15} {'Difference':<15}")
print("-" * 65)
print(f"{'Average CPU (%)':<20} {docker_avg:<15.2f} {k8s_avg:<15.2f} {abs(docker_avg - k8s_avg):<15.2f}")
print(f"{'Maximum CPU (%)':<20} {docker_max:<15.2f} {k8s_max:<15.2f} {abs(docker_max - k8s_max):<15.2f}")

# Save the comparison to a file
with open("output/cpu_comparison_results.txt", "w") as f:
    f.write("CPU Usage Comparison:\n")
    f.write(f"{'Metric':<20} {'Docker Compose':<15} {'Kubernetes':<15} {'Difference':<15}\n")
    f.write("-" * 65 + "\n")
    f.write(f"{'Average CPU (%)':<20} {docker_avg:<15.2f} {k8s_avg:<15.2f} {abs(docker_avg - k8s_avg):<15.2f}\n")
    f.write(f"{'Maximum CPU (%)':<20} {docker_max:<15.2f} {k8s_max:<15.2f} {abs(docker_max - k8s_max):<15.2f}\n")

print("\nComparison complete. Results saved to output/cpu_comparison_results.txt")
print("Comparison graph saved to output/cpu_comparison.png")
