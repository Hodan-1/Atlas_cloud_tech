# common/constants.py
from __init__ import MeV, GeV

path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
# Sample definitions
SAMPLES = {
    'data': {
        'color': 'black',
        'label': 'Data'
    },
    r'Signal ($m_H$ = 125 GeV)': {
        'color': 'red',
        'label': r'Signal ($m_H$ = 125 GeV)'
    },
    r'Background $ZZ^*$': {
        'color': 'blue',
        'label': r'Background $ZZ^*$'
    },
    r'Background $Z,t\bar{t}$': {
        'color': 'green',
        'label': r'Background $Z,t\bar{t}$'
    }
}

# File to sample mapping
FILE_TO_SAMPLE = {
    'data.root': 'data',
    'signal.root': r'Signal ($m_H$ = 125 GeV)',
    'background_zz.root': r'Background $ZZ^*$',
    'background_other.root': r'Background $Z,t\bar{t}$'
}

# Histogram configuration
HIST_CONFIG = {
    'xmin': 80 * GeV,
    'xmax': 250 * GeV,
    'step_size': 5 * GeV
}
