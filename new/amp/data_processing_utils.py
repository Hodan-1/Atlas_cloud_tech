import numpy as np
import awkward as ak
import vector
from constants import MeV, GeV

def cut_lep_type(lep_type):
    """Cut lepton type (electron type is 11, muon type is 13)"""
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)
    return lep_type_cut_bool

def cut_lep_charge(lep_charge):
    """Cut lepton charge"""
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    return sum_lep_charge

def calc_mass(lep_pt, lep_eta, lep_phi, lep_E):
    """Calculate invariant mass of the 4-lepton state"""
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    invariant_mass = (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV
    return invariant_mass

def calc_weight(weight_variables, sample, events, lumi=10):
    """Calculate event weights for MC samples"""
    import infofile  # Ensure this is imported or available
    info = infofile.infos[sample]
    xsec_weight = (lumi * 1000 * info["xsec"]) / (info["sumw"] * info["red_eff"])
    total_weight = xsec_weight
    for variable in weight_variables:
        total_weight = total_weight * events[variable]
    return total_weight

def process_data(tree, sample_name, is_mc=False, lumi=10, pt_cuts=None, fraction=1.0):
    """Process data from a ROOT file"""
    variables = ['lep_pt', 'lep_eta', 'lep_phi', 'lep_E', 'lep_charge', 'lep_type']
    weight_variables = ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]
    
    sample_data = []
    
    for data in tree.iterate(variables + (weight_variables if is_mc else []), 
                              library="ak", 
                              entry_stop=tree.num_entries * fraction,
                              step_size=1000000):
        # Apply cuts and process data similar to original implementation
        data['leading_lep_pt'] = data['lep_pt'][:, 0]
        data['sub_leading_lep_pt'] = data['lep_pt'][:, 1]
        data['third_leading_lep_pt'] = data['lep_pt'][:, 2]
        data['last_lep_pt'] = data['lep_pt'][:, 3]
        
        # Apply pT cuts if specified
        if pt_cuts:
            data = data[data['leading_lep_pt'] * MeV > pt_cuts[0]]
            data = data[data['sub_leading_lep_pt'] * MeV > pt_cuts[1]]
            data = data[data['third_leading_lep_pt'] * MeV > pt_cuts[2]]
        
        # Apply lepton type and charge cuts
        data = data[~cut_lep_type(data['lep_type'])]
        data = data[~cut_lep_charge(data['lep_charge'])]
        
        # Calculate invariant mass
        data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])
        
        # Calculate weights for MC samples
        if is_mc:
            data['totalWeight'] = calc_weight(weight_variables, sample_name, data, lumi)
        
        sample_data.append(data)
    
    return ak.concatenate(sample_data) if sample_data else None
import numpy as np
import awkward as ak

def prepare_plot_data(all_data, samples, bin_edges):
    """Prepare data for plotting"""
    # Data
    data_x, _ = np.histogram(ak.to_numpy(all_data['data']['mass']), bins=bin_edges)
    data_x_errors = np.sqrt(data_x)
    
    # Signal
    signal_x = ak.to_numpy(all_data[r'Signal ($m_H$ = 125 GeV)']['mass'])
    signal_weights = ak.to_numpy(all_data[r'Signal ($m_H$ = 125 GeV)'].totalWeight)
    signal_color = samples[r'Signal ($m_H$ = 125 GeV)']['color']
    
    # Background MC
    mc_x = []
    mc_weights = []
    mc_colors = []
    mc_labels = []
    
    for s in samples:
        if s not in ['data', r'Signal ($m_H$ = 125 GeV)']:
            mc_x.append(ak.to_numpy(all_data[s]['mass']))
            mc_weights.append(ak.to_numpy(all_data[s].totalWeight))
            mc_colors.append(samples[s]['color'])
            mc_labels.append(s)
    
    return {
        'data_x': data_x,
        'data_x_errors': data_x_errors,
        'signal_x': signal_x,
        'signal_weights': signal_weights,
        'signal_color': signal_color,
        'mc_x': mc_x,
        'mc_weights': mc_weights,
        'mc_colors': mc_colors,
        'mc_labels': mc_labels
    }

def calculate_signal_significance(signal_tot, mc_x_tot, bin_indices):
    """Calculate signal significance"""
    # Ensure inputs are numpy arrays
    signal_tot = np.array(signal_tot)
    mc_x_tot = np.array(mc_x_tot)
    
    # Calculate signal and background in specified bin indices
    N_sig = signal_tot[bin_indices].sum()
    N_bg = mc_x_tot[bin_indices].sum()
    
    # Calculate signal significance
    # Using the standard formula: S / sqrt(B + (0.3 * B)^2)
    signal_significance = N_sig / np.sqrt(N_bg + 0.3 * N_bg**2)
    
    return N_sig, N_bg, signal_significance