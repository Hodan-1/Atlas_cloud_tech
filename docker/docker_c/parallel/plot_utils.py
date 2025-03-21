import matplotlib.pyplot as plt
import numpy as np
import awkward as ak
from matplotlib.ticker import AutoMinorLocator

class AtlasPlotter:
    def __init__(self, xmin=80, xmax=250, step=5, lumi=10):
        self.xmin = xmin
        self.xmax = xmax
        self.step = step
        self.lumi = lumi
        self.bin_edges = np.arange(xmin, xmax + step, step)
        self.bin_centers = (self.bin_edges[:-1] + self.bin_edges[1:]) / 2

    def plot_data(self, data):
        """Plot the data points."""
        counts, _ = np.histogram(ak.to_numpy(data['mass']), bins=self.bin_edges)
        plt.errorbar(self.bin_centers, counts, yerr=np.sqrt(counts), fmt='ko', label='Data')

    def plot_mc(self, backgrounds):
        """Plot the MC background."""
        for bg in backgrounds:
            counts, _ = np.histogram(ak.to_numpy(bg['mass']), bins=self.bin_edges,
                                    weights=ak.to_numpy(bg.totalWeight))
            plt.hist(self.bin_centers, bins=self.bin_edges, weights=counts, alpha=0.5, label='Background')

    def plot_signal(self, signals):
        """Plot the signal."""
        for sig in signals:
            counts, _ = np.histogram(ak.to_numpy(sig['mass']), bins=self.bin_edges,
                                    weights=ak.to_numpy(sig.totalWeight))
            plt.hist(self.bin_centers, bins=self.bin_edges, weights=counts, color='blue', label='Signal')

    def finalize_plot(self):
        """Add labels, legends, and styling to the plot."""
        plt.xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]')
        plt.ylabel(f'Events / {self.step} GeV')
        plt.legend()
        plt.show()