from dotenv import load_dotenv
from os import getenv

load_dotenv()

AWS_SERVER_PUBLIC_KEY = getenv('AWS_SERVER_PUBLIC_KEY')
AWS_SERVER_SECRET_KEY = getenv('AWS_SERVER_SECRET_KEY')
REGION_NAME = getenv('REGION_NAME')
