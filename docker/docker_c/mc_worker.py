import json
import uproot
import awkward as ak
import numpy as np
import vector
import pika
from calc_utils import cut_lep_type, cut_lep_charge, calc_mass, calc_weight
import infofile

def process_mc(sample):
    path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
    dsid = infofile.infos[sample]["DSID"]
    file_path = f"{path}MC/mc_{dsid}.{sample}.4lep.root"
    
    # Your original MC processing logic
    tree = uproot.open(file_path + ":mini")
    weight_vars = ["mcWeight", "scaleFactor_PILEUP", 
                  "scaleFactor_ELE", "scaleFactor_MUON", 
                  "scaleFactor_LepTRIGGER"]
    
    processed_data = []
    for data in tree.iterate(variables + weight_vars, library="ak", step_size=1000000):
        # Apply cuts and weights
        data = data[~cut_lep_type(data['lep_type'])]
        data = data[~cut_lep_charge(data['lep_charge'])]
        
        data['mass'] = calc_mass(...)
        data['totalWeight'] = calc_weight(...)
        
        processed_data.append(data)
    
    ak.to_parquet(ak.concatenate(processed_data),
                 f"/shared_storage/{sample}_mc.parquet")

def main():
    ...

if __name__ == "__main__":
    main()