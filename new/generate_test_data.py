#!/usr/bin/env python
# generate_test_data.py
import os
import numpy as np
import uproot
import awkward as ak
from pathlib import Path

# Constants
MeV = 1
GeV = 1000 * MeV

def generate_test_file(filename, sample_type, n_events=1000):
    """Generate a test ROOT file with synthetic data"""
    print(f"Generating {sample_type} test file with {n_events} events: {filename}")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Generate random data
    if sample_type == "data":
        # Data has a mix of background and signal
        mass = np.random.normal(125*GeV, 15*GeV, n_events)
        # Add some background
        bg_indices = np.random.choice(n_events, size=int(n_events*0.7), replace=False)
        mass[bg_indices] = np.random.exponential(50*GeV, size=len(bg_indices))
    elif sample_type == "signal":
        # Signal is peaked at 125 GeV
        mass = np.random.normal(125*GeV, 5*GeV, n_events)
    elif sample_type == "background_zz":
        # ZZ background is broadly distributed
        mass = np.random.normal(150*GeV, 30*GeV, n_events)
    elif sample_type == "background_other":
        # Other background is exponentially distributed
        mass = np.random.exponential(50*GeV, n_events)
        # Filter out masses below 80 GeV
        mass = mass + 80*GeV
    
    # Generate lepton kinematics
    n_leptons = np.random.randint(2, 5, n_events)
    
    # Create arrays for each lepton property
    lepton_pt = []
    lepton_eta = []
    lepton_phi = []
    lepton_charge = []
    
    for i in range(n_events):
        n_lep = n_leptons[i]
        
        # Generate pT values with leading lepton having higher pT
        pt_values = np.sort(np.random.exponential(20*GeV, n_lep))[::-1]
        
        # Generate other properties
        eta_values = np.random.normal(0, 1.5, n_lep)
        phi_values = np.random.uniform(-np.pi, np.pi, n_lep)
        charge_values = np.random.choice([-1, 1], n_lep)
        
        lepton_pt.append(pt_values)
        lepton_eta.append(eta_values)
        lepton_phi.append(phi_values)
        lepton_charge.append(charge_values)
    
    # Create awkward arrays
    data = {
        "mass": ak.Array(mass),
        "lepton_pt": ak.Array(lepton_pt),
        "lepton_eta": ak.Array(lepton_eta),
        "lepton_phi": ak.Array(lepton_phi),
        "lepton_charge": ak.Array(lepton_charge),
        "totalWeight": ak.Array(np.ones(n_events))
    }
    
    # Extract individual lepton pT for the first four leptons (padding with zeros if needed)
    for i, name in enumerate(["leading_lep_pt", "sub_leading_lep_pt", "third_leading_lep_pt", "last_lep_pt"]):
        data[name] = ak.Array([
            lep_pts[i] if i < len(lep_pts) else 0 
            for lep_pts in lepton_pt
        ])
    
    # Write to ROOT file
    with uproot.recreate(filename) as f:
        f["events"] = data
    
    print(f"Created {filename}")
    return filename

def main():
    """Generate test ROOT files for all sample types"""
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Generate files for each sample type
    files = []
    
    # Data
    files.append(generate_test_file(
        str(data_dir / "data.root"), 
        "data", 
        n_events=5000
    ))
    
    # Signal
    files.append(generate_test_file(
        str(data_dir / "signal.root"), 
        "signal", 
        n_events=2000
    ))
    
    # Background ZZ
    files.append(generate_test_file(
        str(data_dir / "background_zz.root"), 
        "background_zz", 
        n_events=3000
    ))
    
    # Background other
    files.append(generate_test_file(
        str(data_dir / "background_other.root"), 
        "background_other", 
        n_events=4000
    ))
    
    print(f"Generated {len(files)} test ROOT files in {data_dir}")

if __name__ == "__main__":
    main()
