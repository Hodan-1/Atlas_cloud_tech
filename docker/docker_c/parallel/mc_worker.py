import json
import uproot
import awkward as ak
import numpy as np
import vector
import pika
from calc_utils import cut_lep_type, cut_lep_charge, calc_mass, calc_weight
import infofile


lumi = 10
Mev = 0.001


def process_mc(sample, sample_type):
    path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
    dsid = infofile.infos[sample]["DSID"]
    tree = uproot.open(f"{path}MC/mc_{dsid}.{sample}.4lep.root:mini")
    
    data = tree.arrays(library="ak")
    weight_vars = ["mcWeight", "scaleFactor_PILEUP", 
                  "scaleFactor_ELE", "scaleFactor_MUON", 
                  "scaleFactor_LepTRIGGER"]
    
    variables = ['lep_pt','lep_eta','lep_phi','lep_E','lep_charge','lep_type']

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
        data['totalWeight'] = calc_weight(weight_vars, sample, data)
        
        data['sample_type'] = sample_type

        processed_data.append(data)
    
        ak.to_parquet(processed_data, f"/shared_storage/raw_{sample_type}_{sample}.parquet")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='mc_tasks')
    channel.queue_declare(queue='signal_tasks')
    
    def callback(ch, method, properties, body):
        task = json.loads(body)
        if task['type'] in ['mc', 'signal']:
            process_mc(
                sample=task['sample'],
                sample_type=task['type'])
    

    channel.basic_consume(queue='mc_tasks', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='signal_tasks', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    main()