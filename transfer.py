from sharepoint import EurokinSharePoint
from azure_blob import azure_blob
from pathlib import Path
import json

cwd = Path().cwd()
deliverables_dir = cwd / "Deliverables"

with open("secrets.json") as secrets_file:
    secrets = json.load(secrets_file)
eurokin_secrets = secrets["eurokin"]
eurokin = EurokinSharePoint(secrets=eurokin_secrets)

azure_secrets = secrets["azure"]
eurokin_azure = azure_blob(secrets=azure_secrets)

deliverables = eurokin.get_deliverables_name_list()
already_transferred = eurokin_azure.get_uploaded_deliverables()

# TODO: Consider comparing versions too

to_transfer = list(set(deliverables) - set(already_transferred))
print(to_transfer)
