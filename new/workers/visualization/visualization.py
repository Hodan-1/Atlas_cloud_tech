# visualization.py
import os
import json
import time
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import pika
import awkward as ak
from __init__ import connect_to_rabbitmq, deserialize_data, TASK_QUEUE
from constant import SAMPLES, ANALYSIS_QUEUE, PATH



def setup_histogram_bins(xmin=80*GeV, xmax=250*GeV, step_size=5*GeV):
    """Set up histogram bins"""
    bin_edges = np.arange(start=xmin, stop=xmax+step_size, step=step_size)
    bin_centres = np.arange(start=xmin+step_size/2, stop=xmax+step_size/2, step=step_size)
    return bin_edges, bin_centres

def plot_data_only(results, bin_edges, bin_centres, lumi=10, fraction=1.0):
    """Plot 1: Data-only histogram"""
    # Create figure
    plt.figure(figsize=(10, 8))
    main_axes = plt.gca()
    
    # Calculate data histogram
    data_x, _ = np.histogram(ak.to_numpy(results['data']['mass']), bins=bin_edges)
    data_x_errors = np.sqrt(data_x)
    
    # Plot data points
    main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                      fmt='ko', label='Data')
    
    # Set plot properties
    main_axes.set_xlim(left=bin_edges[0], right=bin_edges[-1])
    main_axes.xaxis.set_minor_locator(AutoMinorLocator())
    main_axes.tick_params(which='both', direction='in', top=True, right=True)
    main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
                        fontsize=13, x=1, horizontalalignment='right')
    
    step_size = bin_edges[1] - bin_edges[0]
    main_axes.set_ylabel('Events / '+str(step_size/GeV)+' GeV',
                        y=1, horizontalalignment='right')
    main_axes.set_ylim(bottom=0, top=np.amax(data_x)*1.6)
    main_axes.yaxis.set_minor_locator(AutoMinorLocator())
    
    # Draw legend
    main_axes.legend(frameon=False)
    
    # Save plot
    output_dir = '/app/output'
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/plot1_data_only.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    return output_path

def plot_data_with_background(results, bin_edges, bin_centres, lumi=10, fraction=1.0):
    """Plot 2: Data with one background (Zee)"""
    # Create figure
    plt.figure(figsize=(10, 8))
    main_axes = plt.gca()
    
    # Calculate data histogram
    data_x, _ = np.histogram(ak.to_numpy(results['data']['mass']), bins=bin_edges)
    data_x_errors = np.sqrt(data_x)
    
    # Plot data points
    main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                      fmt='ko', label='Data')
    
    # Get Zee background
    background_key = r'Background $Z,t\bar{t}$'
    mc_x = ak.to_numpy(results[background_key]['mass'])
    mc_weights = ak.to_numpy(results[background_key].totalWeight)
    mc_colors = SAMPLES[background_key]['color']
    mc_labels = "Background $Z \\to ee$"
    
    # Plot the Monte Carlo bars
    mc_heights = main_axes.hist(mc_x, bins=bin_edges,
                               weights=mc_weights, stacked=True,
                               color=mc_colors, label=mc_labels)
    
    mc_x_tot = mc_heights[0]  # stacked background MC y-axis value
    
    # Calculate MC statistical uncertainty
    mc_x_err = np.sqrt(np.histogram(mc_x, bins=bin_edges, weights=mc_weights**2)[0])
    
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
    
    # Draw legend
    main_axes.legend(frameon=False)
    
    # Save plot
    output_dir = '/app/output'
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/plot2_data_with_background.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    return output_path

def plot_full_mass_histogram(results, bin_edges, bin_centres, lumi=10, fraction=1.0):
    """Plot 3: Full mass histogram with all backgrounds and signal"""
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
    output_path = f"{output_dir}/plot3_full_mass_histogram.png"
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
    
    return output_path

def plot_lepton_pt_distributions(results, lumi=10, fraction=1.0):
    """Plot 4: Lepton pT distributions for all four leptons"""
    # Setup
    xmin = 0
    xmax = 200
    step_size = 5 * GeV
    
    bin_edges = np.arange(start=xmin, stop=xmax+step_size, step=step_size)
    bin_centres = np.arange(start=xmin+step_size/2, stop=xmax+step_size/2, step=step_size)
    
    # Get lepton pT data
    pt_keys = ['leading_lep_pt', 'sub_leading_lep_pt', 'third_leading_lep_pt', 'last_lep_pt']
    signal_all = []
    for key in pt_keys:
        signal_all.append(ak.to_numpy(results[r'Signal ($m_H$ = 125 GeV)'][key]*MeV))
    
    signal_weights = ak.to_numpy(results[r'Signal ($m_H$ = 125 GeV)'].totalWeight)
    signal_color = SAMPLES[r'Signal ($m_H$ = 125 GeV)']['color']
    
    mc_all = []
    mc_weights = []
    mc_colors = []
    mc_labels = []
    
    for index in range(4):
        mc_all.append([])
        for s in SAMPLES:
            if s not in ['data', r'Signal ($m_H$ = 125 GeV)']:
                mc_all[index].append(ak.to_numpy(results[s][pt_keys[index]]*MeV))
    
    for s in SAMPLES:
        if s not in ['data', r'Signal ($m_H$ = 125 GeV)']:
            mc_weights.append(ak.to_numpy(results[s].totalWeight))
            mc_colors.append(SAMPLES[s]['color'])
            mc_labels.append(s)
    
    # Create plot
    fig, ax = plt.subplots(4, 1, figsize=(6, 12))
    
    for axis in range(4):
        # Plot the Monte Carlo bars
        ax[axis].hist(mc_all[axis], bins=bin_edges,
                     weights=mc_weights, stacked=False,
                     color=mc_colors, label=mc_labels, histtype='step')
        
        # Plot the signal bar
        ax[axis].hist(signal_all[axis], bins=bin_edges,
                     weights=signal_weights, color=signal_color,
                     label=r'Signal ($m_H$ = 125 GeV)', histtype='step')
        
        # Set plot properties
        ax[axis].set_xlim(left=xmin, right=xmax)
        ax[axis].xaxis.set_minor_locator(AutoMinorLocator())
        ax[axis].tick_params(which='both', direction='in', top=True, right=True)
        ax[axis].set_ylabel('Events / '+str(step_size/GeV)+' GeV',
                          y=1, horizontalalignment='right')
        ax[axis].set_ylim(bottom=0, top=100)
        ax[axis].yaxis.set_minor_locator(AutoMinorLocator())
    
    # Set x-axis labels
    labels = [r'Leading lepton $p_t$ [GeV]', r'Sub-leading lepton $p_t$ [GeV]',
             r'Third-leading lepton $p_t$ [GeV]', r'Last lepton $p_t$ [GeV]']
    for axis in range(4):
        ax[axis].set_xlabel(labels[axis], fontsize=10, x=1, horizontalalignment='right')
    
    # Draw legend
    ax[0].legend(frameon=False)
    
    # Save plot
    output_dir = '/app/output'
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/plot4_lepton_pt_distributions.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    return output_path

def plot_final_analysis(results, bin_edges, bin_centres, lumi=10, fraction=1.0):
    """Plot 5: Final analysis plot (same as plot 3 but with additional annotations)"""
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
    main_axes.hist(signal_x, bins=bin_edges, bottom=mc_x_tot,
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
    output_path = f"{output_dir}/plot5_final_analysis.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    return output_path

def callback(ch, method, properties, body):
    """Process a plot task from the queue"""
    try:
        # Parse task
        task = json.loads(body.decode())
        print("Received plot task")
        
        # Extract data
        results = deserialize_data(task['results'])
        bin_edges = np.array(task['bin_edges'])
        bin_centres = np.array(task['bin_centres'])
        lumi = task.get('lumi', 10)
        fraction = task.get('fraction', 1.0)
        
        # Create all plots
        print("Creating plots...")
        
        # Plot 1: Data-only histogram
        plot1 = plot_data_only(results, bin_edges, bin_centres, lumi, fraction)
        print(f"Created Plot 1: {plot1}")
        
        # Plot 2: Data with one background
        plot2 = plot_data_with_background(results, bin_edges, bin_centres, lumi, fraction)
        print(f"Created Plot 2: {plot2}")
        
        # Plot 3: Full mass histogram
        plot3 = plot_full_mass_histogram(results, bin_edges, bin_centres, lumi, fraction)
        print(f"Created Plot 3: {plot3}")
        
        # Plot 4: Lepton pT distributions
        plot4 = plot_lepton_pt_distributions(results, lumi, fraction)
        print(f"Created Plot 4: {plot4}")
        
        # Plot 5: Final analysis
        plot5 = plot_final_analysis(results, bin_edges, bin_centres, lumi, fraction)
        print(f"Created Plot 5: {plot5}")
        
        print(f"\nPlotting completed. Generated 5 plots.")
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"Error processing plot task: {e}")
        import traceback
        traceback.print_exc()
        # Acknowledge the message even on error to avoid reprocessing
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    """Main function to process plot tasks from the queue"""
    # Connect to RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    
    # Declare queue
    channel.queue_declare(queue=ANALYSIS_QUEUE, durable=True)
    
    # Set prefetch count
    channel.basic_qos(prefetch_count=1)
    
    # Set up consumer
    channel.basic_consume(queue=ANALYSIS_QUEUE, on_message_callback=callback)
    
    print("Plotter worker started. Waiting for tasks...")
    
    # Start consuming
    channel.start_consuming()

if __name__ == "__main__":
    main()


