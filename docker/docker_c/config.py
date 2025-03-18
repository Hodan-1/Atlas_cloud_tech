import os
from dotenv import load_dotenv

load_dotenv()
MeV = 0.001
GeV = 1.0

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
BASE_PATH = os.getenv("BASE_PATH", "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/")
