# workers/visualizer/visualizer.py
import json
import time
import os
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import pika
from common.rabbitmq import connect_to_rabbitmq, deserialize_data
from common.rabbitmq import PLOT_QUEUE
from common.constants import MeV, GeV, SAMPLES

def setup_histogram_bins(xmin=80*GeV, xmax=250*GeV, step_size=5*GeV):
    """Set up histogram bins"""
    bin_edges = np.arange(start=xmin, stop=xmax+step_size, step=step_size)
    bin_centres = np.arange(start=xmin+step_size/2, stop=xmax+step_size/2, step=step_size)
    return bin_edges, bin_centres

def plot_mass_histogram(results, bin_edges, bin_centres, lumi=10, fraction=1.0):
    """Plot mass histogram"""
    # Create figure
    plt.figure(figsize=(10, 8))
    main_axes = plt.gca()
    
    # Calculate data histogram
    data_x, _ = np.histogram(ak.to_numpy(results['data']['mass']), bins=bin_edges)
    data_x_errors = np.sqrt(data_x)
    
    # Plot data points
    main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                      fmt='ko', label='Data')
    
    # Get signal
    signal_x = ak.to_numpy(results[r'Signal ($m_H$ = 125 GeV)']['mass'])
    signal_weights = ak.to_numpy(results[r'Signal ($m_H$ = 125 GeV)'].totalWeight)
    signal_color = SAMPLES[r'Signal ($m_H$ = 125 GeV)']['color']
    
    # Get backgrounds
    mc_x = []
    mc_weights = []
    mc_colors = []
    mc_labels = []
    
    for s in SAMPLES:
        if s not in ['data', r'Signal ($m_H$ = 125 GeV)']:
            mc_x.append(ak.to_numpy(results[s]['mass']))
            mc_weights.append(ak.to_numpy(results[s].totalWeight))
            mc_colors.append(SAMPLES[s]['color'])
            mc_labels.append(s)
    
    # Plot the Monte Carlo bars
    mc_heights = main_axes.hist(mc_x, bins=bin_edges,
                               weights=mc_weights, stacked=True,
                               color=mc_colors, label=mc_labels)
    
    mc_x_tot = mc_heights[0][-1]  # stacked background MC y-axis value
    
    # Calculate MC statistical uncertainty
    mc_x_err = np.sqrt(np.histogram(np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])
    
    # Plot the signal bar
    signal_heights = main_axes.hist(signal_x, bins=bin_edges, bottom=mc_x_tot,
                                   weights=signal_weights, color=signal_color,
                                   label=r'Signal ($m_H$ = 125 GeV)')
    
    # Plot statistical uncertainty
    step_size = bin_edges[1] - bin_edges[0]
    main_axes.bar(bin_centres, 2*mc_x_err, alpha=0.5,
                 bottom=mc_x_tot-mc_x_err, color='none',
                 hatch="////", width=step_size, label='Stat. Unc.')
    
    # Set plot properties
    main_axes.set_xlim(left=bin_edges[0], right=bin_edges[-1])
    main_axes.xaxis.set_minor_locator(AutoMinorLocator())
    main_axes.tick_params(which='both', direction='in', top=True, right=True)
    main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
                        fontsize=13, x=1, horizontalalignment='right')
    main_axes.set_ylabel('Events / '+str(step_size/GeV)+' GeV',
                        y=1, horizontalalignment='right')
    main_axes.set_ylim(bottom=0, top=np.amax(data_x)*1.6)
    main_axes.yaxis.set_minor_locator(AutoMinorLocator())
    
    # Add text to plot
    plt.text(0.05, 0.93, 'ATLAS Open Data', transform=main_axes.transAxes, fontsize=13)
    plt.text(0.05, 0.88, 'for education', transform=main_axes.transAxes, style='italic', fontsize=8)
    lumi_used = str(lumi*fraction)
    plt.text(0.05, 0.82, '$\sqrt{s}$=13 TeV,$\int$L dt = '+lumi_used+' fb$^{-1}$', transform=main_axes.transAxes)
    plt.text(0.05, 0.76, r'$H \rightarrow ZZ^* \rightarrow 4\ell$', transform=main_axes.transAxes)
    
    # Draw legend
    main_axes.legend(frameon=False)
    
    # Save plot
    output_dir = '/app/output'
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/mass_histogram.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    # Calculate signal significance
    signal_tot = signal_heights[0] + mc_x_tot
    bin_indices = [7, 8, 9]  # Bins around 125 GeV
    N_sig = signal_tot[bin_indices].sum()
    N_bg = mc_x_tot[bin_indices].sum()
    signal_significance = N_sig / np.sqrt(N_bg + 0.3 * N_bg**2)
    
    print(f"\nResults:")
    print(f"N_sig = {N_sig:.3f}")
    print(f"N_bg = {N_bg:.3f}")
    print(f"Signal significance = {signal_significance:.3f}")
    
    return {
        'output_path': output_path,
        'N_sig': float(N_sig),
        'N_bg': float(N_bg),
        'signal_significance': float(signal_significance)
    }

def callback(ch, method, properties, body):
    """Process a plot task from the queue"""
    try:
        # Parse task
        task = json.loads(body.decode())
        print("Received plot task")
        
        # Extract data
        results = deserialize_data(task['results'])
        lumi = task.get('lumi', 10)
