import os
import datetime
import json

import requests
import pandas as pd

import plotly.express as px
from dotenv import load_dotenv


# Get Environment Variables for Credential Purposes
BASE64_AUTH = os.environ['BASE64_AUTH']


class FuelPriceUpdate:
	"""
	FuelPriceUpdate class

	The purpose of this class is to retrieve latest fuel prices and station
	information from NSW government endpoint, or to retrieve stored records
	from local environment.

	These data will be used in later process to handle user requests, and 
	for other analytical purposes.

	"""

    def __init__(self, authorization, is_update=False):
        self.is_update = is_update
        self.authorization = authorization
        self.access_token = None
        self.raw_response = None
        self.stations_df = None
        self.prices_df = None
        self.combined_df = None

    def get_token(self):
    	"""
		Get Access Token from NSW API

		This token will be expired every 12 hours - a CRON job should be completed
		every 12 hours to ensure latest data being updated.
    	"""
        print('Step 1/6 : Generating Access Token for NSW API...')
        url = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"

        querystring = {"grant_type":"client_credentials"}

        headers = {
            'content-type': "application/json",
            'authorization': self.authorization
            }

        response = requests.request("GET", url, headers=headers, params=querystring)

        self.access_token = response.json()['access_token']
        
    def update_data(self):
    	"""
    	Fetch Latest Data from NSW Government API

    	Using the Access Token Fetched Before to fetch the latest data, including the 
    	station lists and price lists.
    	"""

        print('Step 2/6 : Fetching Fuel Price from NSW API...')
        url = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v2/fuel/prices"

        querystring = {"states":"NSW"}
        headers = {
            'content-type': CONTENT_TYPE,
            'authorization': f'Bearer {self.access_token}',
            'apikey': API_KEY,
            'transactionid': '1',
            'requesttimestamp': CUR_TIME,
            }
        try:
            response = requests.request("GET", url, headers=headers, params=querystring)
            self.raw_response = response.json()
        except:
            print('Failed to fetch fuel prices, please try again later.')
            
    
    def create_price_list(self):
    	"""
	    Creating a Dataframe for Price List
    	"""
        print('Step 3/6 : Generating Fuel Price Dataframe...')
        self.prices_df = pd.DataFrame.from_dict(self.raw_response['prices'])

    # Create Table for Station Mapping
    def create_station_list(self):
    	"""
	    Creating a Dataframe for Stations List
    	"""
        print('Step 4/6 : Generating Station List... ')
        rows = []
        for station in self.raw_response['stations']:
            row = []
            for col, value in station.items():
                if col == 'location':
                    for _, degree in value.items():
                        row.append(degree)
                else:
                    row.append(value)
            rows.append(row)
        df = pd.DataFrame(rows, columns = ['brandid', 'stationid', 'brand', 'code', 'name',
                                                    'address', 'latitude', 'longitude', 'state'])
        df['code'] = df['code'].astype('int64')
        self.stations_df = df.copy()
        
    def build_working_df(self):
    	"""
	    Joining the Price and Station List for further processing.
    	"""    	
        print('Step 5/6 : Building Working Dataframe for Processing... ')
        self.working_df = pd.merge(self.stations_df,
                                   self.prices_df,
                                   how='right',
                                   left_on='code',
                                   right_on='stationcode')
    def save_as_csv(self, path):
    	"""
		Saving the fetched dataframe as CSV in designated path

		input:
		  - path:
		  	destination path for csv export purpose.
		"""
        self.stations_df.to_csv(f'{path}/stations.csv', index=True)
        self.prices_df.to_csv(f'{path}/prices.csv', index=True)
        self.working_df.to_csv(f'{path}/combined.csv', index=True)
        
        print('   - Export Completed.')
    
    def check_folder(self, path):
        if os.path.exists(path):
            self.save_as_csv(path)
        else:
            os.mkdir(path)
            self.save_as_csv(path)
    
    def export(self):
    	"""
		Exporting tables as csv for future use and archive
    	"""

        print('Step 6/6 : Saving Price and Station data as csv file... ')
        today_date = datetime.datetime.now().strftime('%Y%m%d')
        path = f'data/backup_{today_date}'
        if os.path.exists('data'):
            self.check_folder(path)
        else:
            os.mkdir('data')
            self.check_folder(path)
            
    def read_record(self):
    	"""
		Retrieved saved record if an upated is not required
    	"""
        all_subdirs = sorted([d for d in os.listdir('data')], reverse=True)
        latest_folder = all_subdirs[0]
        
        path = f'data/{latest_folder}'
        
        print(f'Step 1/3 : Retriving Fuel Price Data on {latest_folder[-8:]}')
        self.prices_df = pd.read_csv(f'{path}/prices.csv', index_col=False)
        print(f'Step 2/3 : Retriving Fuel Price Data on {latest_folder[-8:]}')
        self.stations_df = pd.read_csv(f'{path}/stations.csv', index_col=False)
        print(f'Step 3/3 : Retriving Fuel Price Data on {latest_folder[-8:]}')
        self.working_df = pd.read_csv(f'{path}/combined.csv', index_col=False)
        
        print('Data retrieved without error.')
            
    def run(self):
    	"""
    	Wrapper Function
    	"""

        print('FuelRun - Backend //')
        if self.is_update:
            print('Downloading and Processing Latest Data - ')
            self.get_token()
            self.update_data()
            self.create_price_list()
            self.create_station_list()
            self.build_working_df()
            self.export()
        else:
            self.read_record()
            