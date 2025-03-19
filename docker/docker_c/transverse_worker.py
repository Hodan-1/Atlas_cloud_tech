import json
import pika
import awkward as ak

lumi = 10
fraction = 1.0

def process_transverse():
    # Load all processed data
    data_files = glob.glob("/shared_storage/*_data.parquet")
    mc_files = glob.glob("/shared_storage/*_mc.parquet")

    all_data = []
    for f in data_files + mc_files:
        df = ak.from_parquet(f)
        # Preserve sample type labels
        if 'sample_type' not in df.fields:
            df['sample_type'] = 'data' if 'data' in f else 'mc'
        all_data.append(df)
    
    combined = ak.concatenate(all_data)    

    # Apply transverse cuts
    cutoffs = [30, 20, 10]  # GeV
    filtered = combined[
        (all_data.leading_lep_pt > cutoffs[0]) &
        (all_data.sub_leading_lep_pt > cutoffs[1]) &
        (all_data.third_leading_lep_pt > cutoffs[2])
    ]
    
    # Save final dataset
    ak.to_parquet(filtered, "/shared_storage/final_dataset.parquet")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='transverse_tasks')
    
    def callback(ch, method, properties, body):
        process_transverse()
        # Trigger visualization service
        
    channel.basic_consume(queue='transverse_tasks', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    main()