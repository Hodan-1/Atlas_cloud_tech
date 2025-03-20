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
        self.bin_edges = np.arange(xmin, xmax+step, step)
        self.bin_centers = (self.bin_edges[:-1] + self.bin_edges[1:])/2
        
    def _base_style(self, ax):
        """Shared styling for all plots (original notebook settings)"""
        ax.xaxis.set_minor_locator(AutoMinorLocator())
        ax.yaxis.set_minor_locator(AutoMinorLocator())
        ax.tick_params(which='both', direction='in', top=True, right=True)
        ax.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]', fontsize=13)
        ax.set_ylabel(f'Events / {self.step} GeV')
        ax.set_xlim(self.xmin, self.xmax)
        
        # ATLAS labels (original positions)
        ax.text(0.05, 0.93, 'ATLAS Open Data', transform=ax.transAxes, fontsize=13)
        ax.text(0.05, 0.88, 'for education', transform=ax.transAxes, style='italic', fontsize=8)
        ax.text(0.05, 0.82, fr'$\sqrt{{s}}$=13 TeV,$\int$L dt = {self.lumi} fb$^{{-1}}$', transform=ax.transAxes)
        ax.text(0.05, 0.76, r'$H \rightarrow ZZ^* \rightarrow 4\ell$', transform=ax.transAxes)
        return ax

    def plot_data(self, data, ax):
        """Data points with error bars (original style)"""
        counts, _ = np.histogram(ak.to_numpy(data['mass']), bins=self.bin_edges)
        ax.errorbar(self.bin_centers, counts, yerr=np.sqrt(counts), 
                   fmt='ko', label='Data')
        return ax

    def plot_mc(self, mc_samples, ax, colors=None, labels=None):
        """Stacked MC with uncertainty (original logic)"""
        colors = colors or ['#6b59d3']*3 + ['#ff0000']
        labels = labels or ['Zee', 'Zmumu', 'ttbar', 'ZZ']
        
        values = [ak.to_numpy(s['mass']) for s in mc_samples]
        weights = [ak.to_numpy(s['totalWeight']) for s in mc_samples]
        
        # Stacked plot
        ax.hist(values, bins=self.bin_edges, weights=weights,
               stacked=True, color=colors, label=labels)
        
        # Statistical uncertainty
        summed_w2 = np.sum([w**2 for w in weights], axis=0)
        mc_err = np.sqrt(np.histogram(np.hstack(values), bins=self.bin_edges,
                                     weights=np.hstack(summed_w2))[0])
        ax.bar(self.bin_centers, 2*mc_err, width=self.step, alpha=0.5,
              bottom=np.hstack(weights).sum()-mc_err, hatch='////',
              color='none', label='Stat. Unc.')
        return ax

    def plot_signal(self, signal, ax):
        """Signal overlay (original color/style)"""
        weights = ak.to_numpy(signal['totalWeight'])
        ax.hist(ak.to_numpy(signal['mass']), bins=self.bin_edges,
               weights=weights, color='#00cdff', 
               label=r'Signal ($m_H=125$ GeV)', zorder=2.5)
        return ax

    def plot_transverse_pt(self, data, cutoffs, ax):
        """Transverse momentum distributions (bonus activity)"""
        pt_keys = ['leading_lep_pt', 'sub_leading_lep_pt', 
                  'third_leading_lep_pt', 'last_lep_pt']
        for i, key in enumerate(pt_keys):
            ax[i].hist(ak.to_numpy(data[key]), bins=np.arange(0, 200+5, 5),
                      histtype='step', label=f'Cut: {cutoffs[i]} GeV')
            ax[i].set_xlabel(f'{key.replace("_", " ").title()} [GeV]')
            ax[i].set_ylabel('Events / 5 GeV')
        return ax

    def create_figure(self):
        """Initialize empty plot with ATLAS styling"""
        fig, ax = plt.subplots()
        self._base_style(ax)
        return fig, ax