import time
import os
import logging

# Import external libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Import local package
from reader import SimulationReader
from core import InternalState
from config import Configuration

def process_sensor_file(sensor_file, config_path):
    """
    Process a single sensor file to detect onset of acceleration (OOA).

    Args:
        sensor_file (str): Path to the input sensor file.
        config_path (str): Path to the configuration file.
    """

    # Load configuration
    c = Configuration(config_path)

    # Override input file dynamically
    c.get()['INPUT']['File'] = sensor_file

    # Setup logger
    log_file_path = c.get_logs_path()
    logging.basicConfig(filename=log_file_path, level=logging.INFO, filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    logger.info(f"Processing file: {sensor_file}")

    # Create reader instance for the current file
    r = SimulationReader(config=c.get())

    # Create instance for storing internal state
    s = InternalState(nrows=r.get_nrows(), SW=c.get_SW(), VW=c.get_VW())

    iteration = 0
    while True:
        try:
            item = r.next()

            # Validate the value in item[1]
            if pd.isna(item[1]):
                logger.warning(f"Invalid data (NaTType or NaN) encountered in file {sensor_file} at iteration {iteration}")
                continue

            # Add observation to internal state's displacement array
            s.arr_disp[iteration] = float(item[1])

            # Apply smoothing and compute velocities
            s.process(iteration)

            # Onset of acceleration detection
            if not s.ooa_detected:
                s.detect_ooa(iteration)

            # Time of failure forecasting
            else:
                s.predict(iteration)

                # Create plots, if enabled
                if c.get_plots_enabled():
                    plot_path = os.path.join(c.get_plot_output_path(), f"sensor_{os.path.basename(sensor_file)}.csv")
                    os.makedirs(plot_path, exist_ok=True)
                    s.plot_le(iteration, plot_path)
                    s.plot_boxplot(iteration, plot_path)
                    s.plot_ooa_criteria(iteration, plot_path)
                    s.plot_invv(iteration, plot_path)
                    s.plot_disp(iteration, plot_path)
                    logger.info(f"Created plots for {sensor_file}")

            iteration += 1

        except StopIteration:
            # Export the internal state to CSV, if enabled
            if c.get_csvdump_enabled():
                output_csv = os.path.join(c.get_csv_output_path(), f"state_{os.path.basename(sensor_file)}.csv")
                s.export(iteration, output_csv)
                logger.info(f"Exported internal state to CSV for {sensor_file}")

            logger.info(f"Finished processing file: {sensor_file}")
            break
        except ValueError as e:
            logger.error(f"ValueError encountered: {e} for file {sensor_file} at iteration {iteration}")
            continue


def main():
    # List of sensor input files
    sensor_files = [
        '../input/CM01-Reading-Frequency_(mm).csv',
        '../input/CM02-Reading-Frequency_(mm).csv',
        '../input/CM03-Reading-Frequency_(mm).csv',
        '../input/CM04-Reading-Frequency_(mm).csv',
        '../input/TM01-Resultant.csv',
        '../input/TM02-Resultant.csv',
        '../input/TM03-Resultant.csv',
        '../input/TM04-Resultant.csv',
        '../input/TM05-Resultant.csv',
        '../input/TM06-Resultant.csv',
        '../input/TM06-Resultant.csv',
        '../input/TM07-Resultant.csv',
        '../input/TM08-Resultant.csv',
        '../input/TM09-Resultant.csv',
        '../input/TM10-Resultant.csv'
    ]

    # Path to configuration file
    config_path = '../configs/default.ini'

    while True:
        # Process all files
        for sensor_file in sensor_files:
            process_sensor_file(sensor_file, config_path)

        # Wait for 4 hours (4 hours = 4 * 60 * 60 seconds)
        print("Waiting for 4 hours before the next run...")
        time.sleep(4*60*60)


if __name__ == "__main__":
    main()
