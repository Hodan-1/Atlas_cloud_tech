# constants.py
import numpy as np
import logging

# Configure logging to output to the console with a basic format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Unit definitions for energy measurements
MeV = 0.001  # 1 MeV = 0.001 GeV
GeV = 1.0    # 1 GeV = 1.0 GeV (base unit)

# ATLAS Open Data directory URL
PATH = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"

# Variables to extract from the ROOT files
VARIABLES = ['lep_pt', 'lep_eta', 'lep_phi', 'lep_E', 'lep_charge', 'lep_type']
WEIGHT_VARIABLES = ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]

# Sample definitions for data, background, and signal
SAMPLES = {
    'data': {
        'list': ['data_A', 'data_B', 'data_C', 'data_D'],  # List of data samples
    },
    r'Background $Z,t\bar{t}$': {
        'list': ['Zee', 'Zmumu', 'ttbar_lep'],  # Background samples from Z and ttbar processes
        'color': "#6b59d3"  # Purple color for plotting
    },
    r'Background $ZZ^*$': {
        'list': ['llll'],  # Background sample from ZZ* process
        'color': "#ff0000"  # Red color for plotting
    },
    r'Signal ($m_H$ = 125 GeV)': {
        'list': ['ggH125_ZZ4lep', 'VBFH125_ZZ4lep', 'WH125_ZZ4lep', 'ZH125_ZZ4lep'],  # Signal samples for Higgs boson
        'color': "#00cdff"  # Light blue color for plotting
    },
}

# RabbitMQ queue names for task distribution and result collection
TASK_QUEUE = 'task_queue'  # Queue for distributing tasks
RESULT_QUEUE = 'result_queue'  # Queue for collecting results
ANALYSIS_QUEUE = 'analysis_queue'  # Queue for analysis tasks
VISUALIZATION_QUEUE = 'visualization_queue'  # Queue for visualization tasks

def setup_histogram_bins(xmin=80*GeV, xmax=250*GeV, step_size=5*GeV):
    """
    Set up histogram bins for analysis.
    
    Args:
        xmin (float): Minimum value for the histogram bins (default: 80 GeV).
        xmax (float): Maximum value for the histogram bins (default: 250 GeV).
        step_size (float): Step size between bins (default: 5 GeV).
    
    Returns:
        tuple: A tuple containing the bin edges and bin centres as numpy arrays.
    """
    # Create bin edges and centres for the histogram
    bin_edges = np.arange(start=xmin, stop=xmax+step_size, step=step_size)
    bin_centres = np.arange(start=xmin+step_size/2, stop=xmax+step_size/2, step=step_size)
    
    logging.debug(f"Histogram bins set up with xmin={xmin}, xmax={xmax}, step_size={step_size}.")
    return bin_edges, bin_centres