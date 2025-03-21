# At the top of data_loader.py or in a separate file
import uproot

# Unit definitions
MeV = 0.001
GeV = 1.0

# ATLAS Open Data directory
path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"

def load_file(sample_type, sample_name):
    """Load a ROOT file and return the tree"""
    try:
        if sample_type == 'data':
            prefix = "Data/"
            file_path = path + prefix + sample_name + ".4lep.root"
        else:
            import infofile
            prefix = "MC/mc_" + str(infofile.infos[sample_name]["DSID"]) + "."
            file_path = path + prefix + sample_name + ".4lep.root"
        
        return uproot.open(file_path + ":mini")
    except Exception as e:
        print(f"Error loading file {sample_name}: {e}")
        return None