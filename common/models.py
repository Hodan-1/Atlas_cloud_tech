# common/models.py
import json
import numpy as np
import awkward as ak
import base64
import pickle

class ProcessingTask:
    def __init__(self, sample_type, sample_name, lumi=10, fraction=1.0, pt_cuts=None):
        self.sample_type = sample_type
        self.sample_name = sample_name
        self.lumi = lumi
        self.fraction = fraction
        self.pt_cuts = pt_cuts
    
    def to_json(self):
        return json.dumps({
            'sample_type': self.sample_type,
            'sample_name': self.sample_name,
            'lumi': self.lumi,
            'fraction': self.fraction,
            'pt_cuts': self.pt_cuts
        })
    
    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls(
            data['sample_type'],
            data['sample_name'],
            data.get('lumi', 10),
            data.get('fraction', 1.0),
            data.get('pt_cuts')
        )

class ProcessingResult:
    def __init__(self, sample_type, sample_name, data=None, error=None):
        self.sample_type = sample_type
        self.sample_name = sample_name
        self.data = data  # awkward array
        self.error = error
    
    def to_json(self):
        # Serialize awkward array to bytes
        data_bytes = None
        if self.data is not None:
            data_bytes = base64.b64encode(pickle.dumps(self.data)).decode('utf-8')
        
        return json.dumps({
            'sample_type': self.sample_type,
            'sample_name': self.sample_name,
            'data': data_bytes,
            'error': self.error
        })
    
    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        result = cls(
            data['sample_type'],
            data['sample_name'],
            error=data.get('error')
        )
        
        # Deserialize awkward array from bytes
        if data.get('data'):
            result.data = pickle.loads(base64.b64decode(data['data']))
        
        return result

class AnalysisTask:
    def __init__(self, all_data, bin_edges, bin_centres, lumi=10, fraction=1.0):
        self.all_data = all_data  # Dictionary of awkward arrays
        self.bin_edges = bin_edges
        self.bin_centres = bin_centres
        self.lumi = lumi
        self.fraction = fraction
    
    def to_json(self):
        # Serialize awkward arrays to bytes
        all_data_bytes = {}
        for key, value in self.all_data.items():
            all_data_bytes[key] = base64.b64encode(pickle.dumps(value)).decode('utf-8')
        
        return json.dumps({
            'all_data': all_data_bytes,
            'bin_edges': self.bin_edges.tolist(),
            'bin_centres': self.bin_centres.tolist(),
            'lumi': self.lumi,
            'fraction': self.fraction
        })
    
    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        
        # Deserialize awkward arrays from bytes
        all_data = {}
        for key, value in data['all_data'].items():
            all_data[key] = pickle.loads(base64.b64decode(value))
        
        return cls(
            all_data,
            np.array(data['bin_edges']),
            np.array(data['bin_centres']),
            data.get('lumi', 10),
            data.get('fraction', 1.0)
        )

class AnalysisResult:
    def __init__(self, plot_data, signal_significance=None, plot_path=None):
        self.plot_data = plot_data
        self.signal_significance = signal_significance
        self.plot_path = plot_path
    
    def to_json(self):
        # Serialize numpy arrays in plot_data
        serialized_plot_data = {}
        for key, value in self.plot_data.items():
            if isinstance(value, np.ndarray):
                serialized_plot_data[key] = value.tolist()
            elif isinstance(value, list) and all(isinstance(x, np.ndarray) for x in value):
                serialized_plot_data[key] = [x.tolist() for x in value]
            else:
                serialized_plot_data[key] = value
        
        return json.dumps({
            'plot_data': serialized_plot_data,
            'signal_significance': self.signal_significance,
            'plot_path': self.plot_path
        })
    
    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        
        # Deserialize numpy arrays in plot_data
        plot_data = {}
        for key, value in data['plot_data'].items():
            if isinstance(value, list) and key in ['data_x', 'data_x_errors', 'signal_weights']:
                plot_data[key] = np.array(value)
            elif isinstance(value, list) and key in ['mc_x', 'mc_weights']:
                plot_data[key] = [np.array(x) for x in value]
            else:
                plot_data[key] = value
        
        return cls(
            plot_data,
            data.get('signal_significance'),
            data.get('plot_path')
        )
