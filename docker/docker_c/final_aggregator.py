import os
import awkward as ak
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

# Read environment variables
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")

# Your existing plotting code here
def final_aggregator(final_data, output_dir):
    mc_x = ak.to_numpy(final_data["mass"])
    mc_weights = ak.to_numpy(final_data["totalWeight"])
    mc_colors = samples["Background $Z,t\\bar{t}$"]['color']
    mc_labels = "Background $Z \\to ee$"

    # Plotting (same as before)
    main_axes = plt.gca()
    main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors, fmt='ko', label='Data')
    mc_heights = main_axes.hist(mc_x, bins=bin_edges, weights=mc_weights, stacked=True, color=mc_colors, label=mc_labels)
    mc_x_tot = mc_heights[0]
    mc_x_err = np.sqrt(np.histogram(np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])
    main_axes.bar(bin_centres, 2*mc_x_err, alpha=0.5, bottom=mc_x_tot-mc_x_err, color='none', hatch="////", width=step_size, label='Stat. Unc.')
    
    # Formatting
    main_axes.set_xlim(left=xmin, right=xmax)
    main_axes.xaxis.set_minor_locator(AutoMinorLocator())
    main_axes.tick_params(which='both', direction='in', top=True, right=True)
    main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]', fontsize=13, x=1, horizontalalignment='right')
    main_axes.set_ylabel('Events / ' + str(step_size) + ' GeV', y=1, horizontalalignment='right')
    main_axes.set_ylim(bottom=0, top=np.amax(data_x)*1.6)
    main_axes.yaxis.set_minor_locator(AutoMinorLocator())
    main_axes.legend(frameon=False)
    
    # Save plot
    plt.savefig(os.path.join(output_dir, "output_plot2.png"))

if __name__ == "__main__":
    final_data = ...  # Load or receive final data
    final_aggregator(final_data, OUTPUT_DIR)
