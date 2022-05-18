from boto3 import client
from requests import request
from datetime import datetime
from io import BytesIO
from pandas import read_json, DataFrame
from credentials import AWS_SERVER_PUBLIC_KEY, AWS_SERVER_SECRET_KEY, REGION_NAME

def camel_to_snake_columns(dataframe: DataFrame):
	columns = []

	for c in dataframe.columns:
		columns.append(c.lower().replace(' ', '_'))

	dataframe.columns = columns

	return dataframe


def extract(url: str):

	headers = {
		"X-RapidAPI-Host": "vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com",
		"X-RapidAPI-Key": "7ddeaba71fmsh7a142fabe12b631p158bd6jsnce5c1d21727d"
	}

	print(f'--> Extracting data from: {url}')

	response = request("GET", url, headers=headers)

	return response.content


def transform(data_raw: bytes) -> DataFrame:
	print('--> Processing data...')

	dataframe: DataFrame = read_json(BytesIO(data_raw))

	dataframe['ingestion_timestamp'] = datetime.now()

	dataframe_casted: DataFrame = dataframe.astype({
		'id': 'string',
		'symbol': 'string',
		'Country': 'string',
		'Continent': 'string',
		'date': 'datetime64[ns]',
		'total_cases': 'int64',
		'new_cases': 'int64',
		'total_deaths': 'int64',
		'new_deaths': 'int64',
		'total_tests': 'int64',
		'new_tests': 'int64',
	})

	dataframe_casted: DataFrame = camel_to_snake_columns(dataframe_casted)

	return dataframe_casted

def load_on_s3(dataframe: DataFrame, path: str):
	print(f'--> Loading data on: {path}')

	buffer = BytesIO()

	dataframe.to_parquet(buffer, index=False)

	s3 = client(
		's3',
		aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
		aws_secret_access_key=AWS_SERVER_SECRET_KEY,
		region_name=REGION_NAME
	)

	bucket = path.split('/')[2]

	try:
		s3.put_object(
			Bucket=bucket,
			Key=path.split(bucket)[1][1:] + f'covid-ovid-data/covid_historic_{datetime.now().date()}',
			Body=buffer.getvalue()
		)
	except Exception as e:
		print(f'--> Error when loading the data: {e}')


if __name__ == "__main__":
	url = "https://vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com/api/covid-ovid-data/"

	data_raw = extract(url)

	dataframe_transformed = transform(data_raw)

	load_on_s3(dataframe_transformed, 's3://datalake-for-covid-anhembi/processed_data/')










