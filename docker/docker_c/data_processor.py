#test functionality
import uproot
import awkward as ak
import logging
import vector  
import infofile
import json

logger = logging.getLogger(__name__)

def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    return (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)

def cut_lep_charge(lep_charge):
    return lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0

def calc_mass(lep_pt, lep_eta, lep_phi, lep_E):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    invariant_mass = (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * 0.001  # Convert to GeV
    return invariant_mass

def calc_weight(sample, data):
    weight_vars = ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]
    weights = ak.prod([data[var] for var in weight_vars if var in data.fields], axis=1)

    # Apply cross-section weight (xsec_weight)
    info = infofile.infos[sample]
    lumi = 10  # fb^-1 for combined data or MC samples

    xsec_weight = (lumi * 1000 * info["xsec"]) / (info["sumw"] * info["red_eff"])  # Convert to pb^-1
    total_weight = xsec_weight * weights

    return total_weight

fraction = 1.0
def process_data(file_path, sample):
    """Reads and processes data from the given ROOT file, returning a dictionary of summaries."""
    try:
        logger.info(f"📂 Opening file: {file_path} (sample: {sample})")

        with uproot.open(file_path) as root_file:
            if "mini" not in root_file:
                raise ValueError(f" Missing 'mini' tree in {file_path}")

            tree = root_file["mini"]

            
            # Define variables to extract
            base_vars = ['lep_pt', 'lep_eta', 'lep_phi', 'lep_E', 'lep_charge', 'lep_type']
            weight_vars = ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]
            variables = base_vars if sample.startswith("data_") else base_vars + weight_vars

            # Read data from ROOT file in chunks (efficient for large files)
            frames = []
            for data in tree.iterate(variables, library="ak", entry_stop=int(tree.num_entries * fraction), step_size=1000000):
                n_events_before = len(data)

                # Apply transverse momentum cuts
                data['leading_lep_pt'] = data['lep_pt'][:, 0]
                data['sub_leading_lep_pt'] = data['lep_pt'][:, 1]
                data['third_leading_lep_pt'] = data['lep_pt'][:, 2]

                data = data[data['leading_lep_pt'] * MeV > cutoffs[0]]
                data = data[data['sub_leading_lep_pt'] * MeV > cutoffs[1]]
                data = data[data['third_leading_lep_pt'] * MeV > cutoffs[2]]

                # Apply selection cuts
                valid_type_mask = cut_lep_type(data['lep_type'])
                valid_charge_mask = cut_lep_charge(data['lep_charge'])
                data = data[valid_type_mask & valid_charge_mask]

                n_events_after = len(data)

                # Compute invariant mass
                data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])

                # Compute weights (only for MC samples)
                data['totalWeight'] = calc_weight(sample, data)

                elapsed_time = round(time.time() - start_time, 1)
                logger.info(f"Processed {sample}: {n_events_before} → {n_events_after} events in {elapsed_time}s")

                frames.append(data)

            # Store all processed data in dictionary format
            if frames:
                final_data = ak.concatenate(frames)
            else:
                final_data = None

            if final_data is None or len(final_data) == 0:
                logger.warning(f" No valid events for {sample}. Skipping.")
                return {"masses": [], "weights": [], "is_data": sample.startswith("data_")}

            return {
                "masses": ak.to_list(final_data["mass"]),
                "weights": ak.to_list(final_data["totalWeight"]),
                "is_data": sample.startswith("data_")
            }

    except Exception as e:
        logger.error(f" Failed to process {file_path}: {e}")
        return None  #  Ensure a dictionary is always returned



