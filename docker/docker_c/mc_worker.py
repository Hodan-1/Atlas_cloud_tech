import json
import uproot
import awkward as ak
import numpy as np
import vector
import pika
from calc_utils import cut_lep_type, cut_lep_charge, calc_mass, calc_weight
import infofile


LUMI_MAP = {
    'data_A': 0.5, 'data_B': 1.9,
    'data_C': 2.9, 'data_D': 4.7
}



def calc_weight(weight_variables, sample, events):
    info = infofile.infos[sample]
    xsec_weight = (lumi*1000*info["xsec"])/(info["sumw"]*info["red_eff"]) #*1000 to go from fb-1 to pb-1
    total_weight = xsec_weight 
    for variable in weight_variables:
        total_weight = total_weight * events[variable]
    return total_weight

def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    return (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)  

def cut_lep_charge(lep_charge):
    return lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0

def calc_mass(lep_pt, lep_eta, lep_phi, lep_E):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    return (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV

def process_mc(sample, data_period):
    path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
    dsid = infofile.infos[sample]["DSID"]
    file_path = f"{path}MC/mc_{dsid}.{sample}.4lep.root"

    tree = uproot.open(file_path + ":mini")
    weight_vars = ["mcWeight", "scaleFactor_PILEUP", 
                  "scaleFactor_ELE", "scaleFactor_MUON", 
                  "scaleFactor_LepTRIGGER"]
    
    processed_data = []
    for data in tree.iterate(variables + weight_vars, library="ak", step_size=1000000):
        data['leading_lep_pt'] = data['lep_pt'][:,0]  
        data['sub_leading_lep_pt'] = data['lep_pt'][:,1]
        data['third_leading_lep_pt'] = data['lep_pt'][:,2]
        data['last_lep_pt'] = data['lep_pt'][:,3]
    
    # Apply cuts and weights
        data = data[~cut_lep_type(data['lep_type'])]
        data = data[~cut_lep_charge(data['lep_charge'])]
        
        data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])
        data['totalWeight'] = calc_weight(weight_variables, value, data)
        
        processed_data.append(data)
    
    ak.to_parquet(ak.concatenate(processed_data),
                 f"/shared_storage/{sample}_mc.parquet")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='data_tasks')
    
    def callback(ch, method, properties, body):
        task = json.loads(body)
        process_data(task['sample'])
    
    channel.basic_consume(queue='data_tasks', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    main()