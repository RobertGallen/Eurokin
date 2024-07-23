from shareplum import Site
from shareplum.site import Version
from requests_ntlm import HttpNtlmAuth
import json

with open("secrets.json") as secrets_file:
    secrets = json.load(secrets_file)

USERNAME = secrets["USERNAME"]
PASSWORD = secrets["PASSWORD"]
SHAREPOINT_SITE = secrets["SHAREPOINT_SITE"]

class SharePoint:
    def auth(self):
        cred = HttpNtlmAuth(USERNAME, PASSWORD)
        self.site = Site(
            SHAREPOINT_SITE,
            auth=cred,
            version=Version.v2007
        )
        return self.site
    
if __name__ == "__main__":
    eurokin = SharePoint().auth()
    lists = eurokin.GetListCollection()
    deliverables_list = eurokin.List("Deliverables_list")
    deliverables = deliverables_list.get_list_items()
    an_id = deliverables[3]["ID"]
    file = deliverables_list.GetAttachmentCollection(an_id)
    print(eurokin)