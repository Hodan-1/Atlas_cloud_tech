

# common/constants.py
import numpy as np

# Unit definitions
MeV = 0.001
GeV = 1.0

# ATLAS Open Data directory
PATH = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"

# Variables to extract from the ROOT files
VARIABLES = ['lep_pt', 'lep_eta', 'lep_phi', 'lep_E', 'lep_charge', 'lep_type']
WEIGHT_VARIABLES = ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]

# Sample definitions
SAMPLES = {
    'data': {
        'list': ['data_A', 'data_B', 'data_C', 'data_D'],
    },
    r'Background $Z,t\bar{t}$': {
        'list': ['Zee', 'Zmumu', 'ttbar_lep'],
        'color': "#6b59d3"  # purple
    },
    r'Background $ZZ^*$': {
        'list': ['llll'],
        'color': "#ff0000"  # red
    },
    r'Signal ($m_H$ = 125 GeV)': {
        'list': ['ggH125_ZZ4lep', 'VBFH125_ZZ4lep', 'WH125_ZZ4lep', 'ZH125_ZZ4lep'],
        'color': "#00cdff"  # light blue
    },
}

# RabbitMQ queue names
TASK_QUEUE = 'task_queue'
RESULT_QUEUE = 'result_queue'
ANALYSIS_QUEUE = 'analysis_queue'
VISUALIZATION_QUEUE = 'visualization_queue'

# Histogram settings
def setup_histogram_bins(xmin=80*GeV, xmax=250*GeV, step_size=5*GeV):
    """Set up histogram bins"""
    bin_edges = np.arange(start=xmin, stop=xmax+step_size, step=step_size)
    bin_centres = np.arange(start=xmin+step_size/2, stop=xmax+step_size/2, step=step_size)
    return bin_edges, bin_centres
