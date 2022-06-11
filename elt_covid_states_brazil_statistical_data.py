from datetime import datetime
from io import BytesIO
from os import getenv
from awswrangler import s3
from boto3 import Session
from dotenv import load_dotenv
from pandas import DataFrame, read_json
from requests import request

load_dotenv()

def camel_to_snake_columns(dataframe: DataFrame):
	columns = []

	for c in dataframe.columns:
		columns.append(c.lower().replace(' ', '_'))

	dataframe.columns = columns

	return dataframe

def extract(url: str):

	headers = {
		"X-RapidAPI-Host": "vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com",
		"X-RapidAPI-Key": "d0f47450b4msh57ad9023ad21c5ep197e65jsnc240663b5ea5"
	}

	print(f'--> Extracting data from: {url}')

	response = request("GET", url, headers=headers)

	return response.content


def transform(data_raw: bytes) -> DataFrame:
	print('--> Processing data...')

	dataframe: DataFrame = read_json(BytesIO(data_raw))

	dataframe['ingestion_timestamp'] = datetime.now()

	dataframe_casted: DataFrame = dataframe.astype({
		'name': 'string',
		'province': 'string',
		'TwoLetterSymbol': 'string',
		'iso': 'string',
		'date': 'datetime64[ns]',
		'confirmed': 'int64',
		'recovered': 'int64',
		'deaths': 'int64',
		'Case_Fatality_Rate': 'float64',
		'confirmed_diff': 'int64',
		'deaths_diff': 'int64',
		'recovered_diff': 'int64',
		'active': 'int64',
		'active_diff': 'int64',
		'fatality_rate': 'float64',
		'Recovery_Proporation': 'int64',
		'ingestion_timestamp': 'datetime64[ns]',
	})

	dataframe_casted: DataFrame = camel_to_snake_columns(dataframe_casted)

	return dataframe_casted

def load_on_s3(dataframe: DataFrame, path: str):
	print(f'--> Loading data on: {path}')

	session = Session(
		aws_access_key_id=getenv('AWS_SERVER_PUBLIC_KEY'),
		aws_secret_access_key=getenv('AWS_SERVER_SECRET_KEY'),
		region_name=getenv('REGION_NAME')
	)

	try:
		s3.to_parquet(
			df=dataframe,
			dataset=True,
			path=path,
			boto3_session=session
		)

		print('--> Loaded successfully')
	except Exception as e:
		print(f'--> Error when loading the data: {e}')


def lambda_handler(event=None, context=None):
	url = "https://vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com/api/api-covid-data/reports/BRA"

	data_raw = extract(url)

	dataframe_transformed = transform(data_raw)

	load_on_s3(dataframe_transformed, 's3://datalake-for-covid-anhembi/processed_data/api-covid-data/reports/BRA')
