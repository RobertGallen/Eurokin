from shareplum import Site
from shareplum.site import Version
from requests_ntlm import HttpNtlmAuth
from pathlib import Path
from azure_blob import azure_blob
import aiohttp
import asyncio
import requests
import json


class EurokinSharePoint:
    def __init__(self, secrets: dict) -> Site:
        if {"USERNAME", "PASSWORD", "SHAREPOINT_SITE"} <= set(secrets):
            USERNAME = secrets["USERNAME"]
            PASSWORD = secrets["PASSWORD"]
            SHAREPOINT_SITE = secrets["SHAREPOINT_SITE"]
        else:
            raise KeyError(
                "Secrets dict must contain USERNAME, PASSWORD and SHAREPOINT_SITE"
            )
        self.cred = HttpNtlmAuth(USERNAME, PASSWORD)
        self.site = Site(SHAREPOINT_SITE, auth=self.cred, version=Version.v2007)
        self.site_url = SHAREPOINT_SITE
        self.deliverables_list = None
        return None

    def get_deliverables_list(self) -> list:
        if self.deliverables_list is None:
            self.deliverables_list = self.site.List("Deliverables_list")
        return self.deliverables_list.GetListItems()

    def get_deliverables_name_list(self) -> list:
        deliverables_list = self.get_deliverables_list()
        deliverables_name_list = [d["Name"] for d in deliverables_list]
        return deliverables_name_list

    def get_site_lists(self):
        return self.site.GetListCollection()

    def get_deliverable_path(self, id: int):
        deliverables_list = self.get_deliverables_list()
        return (
            self.site_url + deliverables_list[id]["URL Path"].split("misc/eurokin")[1]
        )

    def request_deliverable(self, id: int):
        url = self.get_deliverable_path(id)
        return requests.get(url=url, auth=self.cred)

    def download_deliverable(self, id: int, output_dir: Path = None):
        deliverables_list = self.get_deliverables_list()
        file_name = deliverables_list[id]["Name"]
        deliverable = self.request_deliverable(id)
        if output_dir is None:
            output_file = file_name
        else:
            output_file = output_dir / file_name
        try:
            with open(output_file, "wb") as f:
                f.write(deliverable.content)
        except FileNotFoundError:
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / file_name
            with open(output_file, "wb") as f:
                f.write(deliverable.content)

    async def transfer_to_azure(
        self, to_be_transferred: list, blob_storage: azure_blob
    ):
        deliverables_list = self.get_deliverables_list()
        ids = []
        for deliverable in deliverables_list:
            if deliverable["Name"] in set(to_be_transferred):
                ids.append(deliverable["id"])
        async with aiohttp.ClientSession() as session:
            for id in ids:
                url = self.get_deliverable_path(id)
                content_name = deliverables_list[id]["Name"]
                async with session.get(url=url, auth=self.cred) as response:
                    blob_storage.upload_content(
                        name=content_name, content=response.content
                    )

        pass


if __name__ == "__main__":
    cwd = Path().cwd()
    deliverables_dir = cwd / "Deliverables"

    with open("secrets.json") as secrets_file:
        secrets = json.load(secrets_file)
    eurokin_secrets = secrets["eurokin"]
    eurokin = EurokinSharePoint(eurokin_secrets)

    deliverables = eurokin.get_deliverables_list()
    with open("deliverables.json", "wt") as json_file:
        json_file.write(json.dumps(deliverables, indent=4))

    collections = eurokin.get_site_lists()
    with open("collections.json", "wt") as json_file:
        json_file.write(json.dumps(collections, indent=4))

    eurokin.download_deliverable(id=100, output_dir=deliverables_dir)
