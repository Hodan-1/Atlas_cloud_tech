import json
import pika
import awkward as ak
import numpy as np
import matplotlib.pyplot as plt
from plot_utils import AtlasPlotter
from config import *
import glob

lumi = 10
fraction = 1.0
MeV = 0.001
cutoffs = [30, 20, 10]

def apply_transverse_cuts(data):
    return data[
        (data.leading_lep_pt * MeV > cutoffs[0]) & 
        (data.sub_leading_lep_pt * MeV > cutoffs[1]) &
        (data.third_leading_lep_pt * MeV > cutoffs[2])]
        
def calculate_significance(signal, backgrounds):
    sig_counts = np.histogram(ak.to_numpy(signal['mass']), bins=bin_edges,
                            weights=ak.to_numpy(signal.totalWeight))[0][7:10].sum()
    bg_counts = sum(np.histogram(ak.to_numpy(bg['mass']), bins=bin_edges,
                                  weights=ak.to_numpy(bg.totalWeight))[0][7:10].sum()
                                  for bg in backgrounds)
    return sig_counts / np.sqrt(bg_counts + 0.3 * bg_counts**2)
        


def process_transverse():
    # Check if data files exist
    data_files = glob.glob("/shared_storage/raw_data_*.parquet")
    if not data_files:
        print("No data files found. Waiting for data processing to complete.")
        return
    
    # Load data
    data = ak.concatenate([ak.from_parquet(f) for f in data_files])
    
    # Check if MC files exist
    mc_files = glob.glob("/shared_storage/raw_background_*.parquet")
    if not mc_files:
        print("No MC background files found. Waiting for MC processing to complete.")
        return
    
    # Load MC backgrounds
    backgrounds = [ak.from_parquet(f) for f in mc_files]
    
    # Check if signal files exist
    signal_files = glob.glob("/shared_storage/raw_signal_*.parquet")
    if not signal_files:
        print("No signal files found. Waiting for signal processing to complete.")
        return
    
    # Load signals
    signals = [ak.from_parquet(f) for f in signal_files]
    
    # Proceed with analysis
    plotter = AtlasPlotter()
    plotter.plot_data(data)
    plotter.plot_mc(backgrounds)
    plotter.plot_signal(signals)
    plotter.save("/shared_storage/final_plot.pdf")
    

      # 2. Transverse momentum distributions (original bonus activity)
    fig, axs = plt.subplots(4, 1, figsize=(6,12))
    pt_vars = ['leading_lep_pt', 'sub_leading_lep_pt', 
              'third_leading_lep_pt', 'last_lep_pt']
    for i, var in enumerate(pt_vars):
        axs[i].hist(
            ak.to_numpy(filtered_data[var]), 
            bins=np.arange(0, 200+5, 5),
            histtype='step',
            label=f'Cut: {cutoffs[i]} GeV'
        )
        axs[i].set_xlabel(f'{var.replace("_", " ").title()} [GeV]')
    plt.savefig('/shared_storage/transverse_pt_distributions.pdf')
    

    # Calculate and print significance
    sig = calculate_significance(filtered_signal, filtered_bg)
    print(f"Signal significance: {sig:.3f}σ")
    
    

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='transverse_tasks')
    
    def callback(ch, method, properties, body):
        process_transverse()
        # Trigger visualization service
        
    channel.basic_consume(queue='transverse_tasks', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    main()