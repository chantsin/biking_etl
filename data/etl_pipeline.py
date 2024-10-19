import pandas as pd
import os

path = "/Users/ChanFamily/data_science_projects/biking_etl_practice/data"

class DataPreprocess:
    def __init__(self, path_folder=path):

        # Path to input
        self.path_input_folder = "{}/input/".format(path_folder)
        self.path_input_bike = self.path_input_folder + 'biking_trip_2021_12.csv'
        
        # Path for output table to be saved on
        self.path_output_folder = "{}/output/".format(path_folder)
        self.path_output_bike = self.path_output_folder + 'biking_trip_2021_12_cleaned.csv'

        # Create folders for output if it does not exists yet
        if not os.path.exists(self.path_output_folder):
            os.makedirs(self.path_output_folder)

    def read_data_from_raw_input(self):

        print("Reading in biking dataset")
        self.data = pd.read_csv(self.path_input_bike)
        print("Finished reading in biking dataset")

    def preprocess_data(self, save_preprocess_data=True):

        print("Start preprocessing biking data")
        self.preprocess_bike()
        print("Finished preprocessing biking data")
        
        if save_preprocess_data:
            print("Saving biking data")
            self.data.to_csv(self.path_output_bike, index=False)
            print("Saved biking data")
        
        return self.data

    def preprocess_bike(self):

        # Drop null rows
        self.data.dropna(inplace=True, ignore_index=True)

        # Change date columns to datetime format
        self.data['started_at'] = pd.to_datetime(self.data['started_at'])
        self.data['ended_at'] = pd.to_datetime(self.data['ended_at'])

        # Gather rows in end_station_id that are not in start_station_id to drop
        self.rows_drop = self.data[
            ~self.data['end_station_id'].isin(self.data['start_station_id'])
        ].index

        # Add rows that are in start_station_id that are not in end_station_id
        self.rows_drop = self.rows_drop.insert(
            len(self.rows_drop),
            self.data[
                ~self.data['start_station_id'].isin(self.data['end_station_id'])
            ].index
        )

        # Drop the rows defined above, reset index
        self.data = self.data.drop(self.rows_drop).reset_index(drop=True)

        # Gather rows in start_station_name that is not in end_station_name
        self.rows_drop = self.data[~self.data['start_station_name'].isin(
            self.data['end_station_name']
        )].index

        # Drop the rows defined above, reset index
        self.data = self.data.drop(self.rows_drop).reset_index(drop=True)

    def read_preprocessed_tables(self):

        print("Reading in modified biking dataset")
        self.data = pd.read_csv(self.path_output_bike)
        print("Finished reading in modified biking dataset")

        return self.data

def main():

    datapreprocessor = DataPreprocess()
    datapreprocessor.read_data_from_raw_input()
    datapreprocessor.preprocess_data()
    print("ETL complete!")

if __name__ == '__main__':
    main()