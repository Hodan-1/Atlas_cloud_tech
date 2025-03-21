# analysis_aggregator.py
import json
import numpy as np
import awkward as ak
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import pika
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

class AnalysisAggregator:
    def __init__(self):
        self.all_data = {
            'data': None,
            'signal': None,
            'background': {}
        }
        self.expected_samples = {
            'data': 4,  # data_A to data_D
            'signal': 4,  # 4 signal samples
            'background': 3  # Z+ttbar, ZZ*, etc.
        }
        self.received_samples = {
            'data': 0,
            'signal': 0,
            'background': 0
        }

    def process_message(self, ch, method, properties, body):
        try:
            message = json.loads(body)
            sample_type = properties.headers.get('sample_type', 'background')
            
            if sample_type == 'data':
                self.handle_data(message)
            elif 'signal' in sample_type.lower():
                self.handle_signal(message)
            else:
                self.handle_background(message)
                
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            if self.check_completion():
                self.generate_final_analysis()
                
        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def handle_data(self, message):
        data = {
            'mass': ak.from_iter(message['data']),
            'pt_vars': message['pt_variables']
        }
        if self.all_data['data'] is None:
            self.all_data['data'] = data
        else:
            self.all_data['data'] = ak.concatenate(
                [self.all_data['data'], data]
            )
        self.received_samples['data'] += 1

    def handle_signal(self, message):
        signal_data = {
            'mass': ak.from_iter(message['mass']),
            'weights': ak.from_iter(message['weights']),
            'pt_vars': message['pt_variables']
        }
        if self.all_data['signal'] is None:
            self.all_data['signal'] = signal_data
        else:
            self.all_data['signal'] = ak.concatenate(
                [self.all_data['signal'], signal_data]
            )
        self.received_samples['signal'] += 1

    def handle_background(self, message):
        sample_name = message['sample']
        bg_data = {
            'mass': ak.from_iter(message['mass']),
            'weights': ak.from_iter(message['weights']),
            'pt_vars': message['pt_variables']
        }
        self.all_data['background'][sample_name] = bg_data
        self.received_samples['background'] += 1

    def check_completion(self):
        return all(
            self.received_samples[k] >= self.expected_samples[k]
            for k in ['data', 'signal', 'background']
        )

    def generate_final_analysis(self):
        logger.info("Generating final analysis plots and significance")
        
        # Convert to numpy arrays for plotting
        data_x = ak.to_numpy(self.all_data['data']['mass'])
        signal_x = ak.to_numpy(self.all_data['signal']['mass'])
        signal_weights = ak.to_numpy(self.all_data['signal']['weights'])
        
        # Process MC backgrounds
        mc_x = []
        mc_weights = []
        mc_colors = []
        mc_labels = []
        
        for sample in self.all_data['background']:
            mc_x.append(ak.to_numpy(self.all_data['background'][sample]['mass']))
            mc_weights.append(ak.to_numpy(self.all_data['background'][sample]['weights']))
            # Get colors from original samples config
            mc_colors.append(samples[sample]['color'])
            mc_labels.append(sample)

        # Plotting code from original analysis
        self.create_main_plot(data_x, signal_x, signal_weights, 
                            mc_x, mc_weights, mc_colors, mc_labels)
        self.calculate_significance()

    def create_main_plot(self, data_x, signal_x, signal_weights, 
                        mc_x, mc_weights, mc_colors, mc_labels):
        plt.figure(figsize=(10, 6))
        main_axes = plt.gca()
        
        # Plot data points
        data_hist, _ = np.histogram(data_x, bins=bin_edges)
        data_err = np.sqrt(data_hist)
        main_axes.errorbar(bin_centres, data_hist, yerr=data_err, 
                         fmt='ko', label='Data')
        
        # Plot MC backgrounds
        mc_heights = main_axes.hist(mc_x, bins=bin_edges, weights=mc_weights,
                                  stacked=True, color=mc_colors, label=mc_labels)
        
        # Plot signal
        signal_heights = main_axes.hist(signal_x, bins=bin_edges, 
                                      weights=signal_weights, 
                                      bottom=np.sum(mc_heights[0], axis=0),
                                      color='#00cdff', 
                                      label=r'Signal ($m_H$ = 125 GeV)')
        
        # Add remaining plot elements
        self.add_plot_elements(main_axes, data_hist)
        plt.show()

    def add_plot_elements(self, axes, data_hist):
        axes.set_xlim(left=xmin, right=xmax)
        axes.xaxis.set_minor_locator(AutoMinorLocator())
        axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]', fontsize=13)
        axes.set_ylabel(f'Events / {step_size} GeV', fontsize=12)
        axes.set_ylim(bottom=0, top=np.amax(data_hist)*1.6)
        
        # Add ATLAS labels
        axes.text(0.05, 0.93, 'ATLAS Open Data', transform=axes.transAxes, fontsize=13)
        axes.text(0.05, 0.88, 'for education', transform=axes.transAxes, style='italic', fontsize=8)
        axes.text(0.05, 0.82, r'$\sqrt{s}=13$ TeV, $\int L\,dt=10$ fb$^{-1}$', 
                transform=axes.transAxes)
        axes.text(0.05, 0.76, r'$H \rightarrow ZZ^* \rightarrow 4\ell$', 
                transform=axes.transAxes)
        axes.legend(frameon=False)

    def calculate_significance(self):
        # Implementation from original code
        signal_tot = signal_heights[0] + mc_x_tot
        N_sig = signal_tot[7:10].sum()
        N_bg = mc_x_tot[7:10].sum()
        significance = N_sig / np.sqrt(N_bg + 0.3 * N_bg**2)
        logger.info(f"Signal significance: {significance:.3f}")

def main():
    aggregator = AnalysisAggregator()
    
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        
        # Declare all queues
        channel.queue_declare(queue='results_queue', durable=True)
        channel.queue_declare(queue='mc_background_tasks', durable=True)
        channel.queue_declare(queue='mc_signal_tasks', durable=True)
        
        # Start consuming
        channel.basic_consume(queue='results_queue', 
                            on_message_callback=aggregator.process_message,
                            auto_ack=False)
        channel.basic_consume(queue='mc_background_tasks', 
                            on_message_callback=aggregator.process_message,
                            auto_ack=False)
        channel.basic_consume(queue='mc_signal_tasks', 
                            on_message_callback=aggregator.process_message,
                            auto_ack=False)
        
        logger.info("Analysis aggregator started")
        channel.start_consuming()
        
    except Exception as e:
        logger.error(f"RabbitMQ connection failed: {str(e)}")

if __name__ == "__main__":
    main()