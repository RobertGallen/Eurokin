from shareplum import Site
from shareplum.site import Version
from requests_ntlm import HttpNtlmAuth
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
        return None

    def get_deliverables_list(self) -> dict:
        deliverables_list = self.site.List("Deliverables_list")
        return deliverables_list.GetListItems()

    def get_site_lists(self):
        return self.GetListCollection()

    def get_deliverable_path(self, id: int):
        deliverables_list = self.get_deliverables_list()
        return self.site_url + deliverables[id]["URL Path"].split("misc/eurokin")[1]

    def request_deliverable(self, id: int):
        url = self.get_deliverable_path(id)
        return requests.get(url=url, auth=self.cred)


if __name__ == "__main__":
    with open("secrets.json") as secrets_file:
        secrets = json.load(secrets_file)
    eurokin = EurokinSharePoint(secrets)

    deliverables = eurokin.get_deliverables_list()
    an_id = deliverables[3]["ID"]
    item_path = eurokin.get_deliverable_path(100)
    item = eurokin.request_deliverable(100)
    with open("test.pdf", "wb") as f:
        f.write(item.content)
    print(item_path)
