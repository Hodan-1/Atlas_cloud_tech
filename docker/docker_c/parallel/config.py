import os
from dotenv import load_dotenv
import numpy as np

load_dotenv()
MeV = 0.001
GeV = 1.0
lumi = 10

cutoffs = [30, 20, 10]  # Transverse momentum cuts in GeV
xmin = 80 * GeV
xmax = 250 * GeV
step_size = 5 * GeV
bin_edges = np.arange(xmin, xmax + step_size, step_size)
bin_centres = (bin_edges[:-1] + bin_edges[1:])/2
signal_region = slice(7, 10)  # 120-135 GeV

samples = {

    'data': {
        'list' : ['data_A','data_B','data_C','data_D'], # data is from 2016, first four periods of data taking (ABCD)
    },

    r'Background $Z,t\bar{t}$' : { # Z + ttbar
        'list' : ['Zee','Zmumu','ttbar_lep'],
        'color' : "#6b59d3" # purple
    },

    r'Background $ZZ^*$' : { # ZZ
        'list' : ['llll'],
        'color' : "#ff0000" # red
    },

    r'Signal ($m_H$ = 125 GeV)' : { # H -> ZZ -> llll
        'list' : ['ggH125_ZZ4lep','VBFH125_ZZ4lep','WH125_ZZ4lep','ZH125_ZZ4lep'],
        'color' : "#00cdff" # light blue
    },

}