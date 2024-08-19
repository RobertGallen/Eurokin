from shareplum import Site
from shareplum import List as ShareplumList
from shareplum.site import Version
from requests_ntlm import HttpNtlmAuth
from pathlib import Path
from azure_blob import AzureBlob
from typing import List, Dict
import asyncio
import requests
import json
import logging

logger = logging.getLogger(__name__)


class EurokinSharePoint:
    """
    A class representing Eurokin SharePoint.

    Args:
        secrets (dict): A dictionary containing the secrets required for authentication.
            It must contain the keys "USERNAME", "PASSWORD", and "SHAREPOINT_SITE".

    Returns:
        None

    Attributes:
        cred (HttpNtlmAuth): The authentication credentials for SharePoint, generated from secrets.
        site (shareplum.Site): The SharePoint site.
        site_url (str): The URL of the SharePoint site.
        deliverables_list (shareplum.List): An object allowing interaction with the deliverables library
                                            (https://shareplum.readthedocs.io/en/latest/objects.html#list)

    """

    def __init__(self, secrets: dict) -> Site:
        """
        Initializes a new instance of the EurokinSharePoint class.

        Args:
            secrets (dict): A dictionary containing the secrets required for authentication.
                It must contain the keys "USERNAME", "PASSWORD", and "SHAREPOINT_SITE".

        Returns:
            EurokinSharePoint: A new instance of the EurokinSharePoint class.

        Raises:
            KeyError: If the secrets dictionary does not contain the required keys.

        """
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

    def get_deliverables_list(self) -> List[Dict]:
        """
        Retrieves the list of deliverables from SharePoint.

        Returns:
            list: a Python list of dicts, each representing a deliverable.

        """
        if self.deliverables_list is None:
            self.deliverables_list = self.site.List("Deliverables_list")
            logger.info("Retrieved current deliverables list from " + self.site_url)
        return self.deliverables_list.GetListItems()

    def get_deliverables_name_list(self) -> List[str]:
        """
        Retrieves the list of deliverable names from SharePoint.

        Returns:
            list: The list of deliverable names.

        """
        deliverables_list = self.get_deliverables_list()
        deliverables_name_list = [d["Name"] for d in deliverables_list]
        return deliverables_name_list

    def get_site_lists(self) -> ShareplumList:
        """
        Retrieves the lists available in the SharePoint site.

        Returns:
            None, popu

        """
        return self.site.GetListCollection()

    def get_deliverable_path(self, id: int):
        """
        Retrieves the path of a deliverable with the specified ID.

        Args:
            id (int): The ID of the deliverable.

        Returns:
            str: The path of the deliverable.

        """
        deliverables_list = self.get_deliverables_list()
        return (
            self.site_url + deliverables_list[id]["URL Path"].split("misc/eurokin")[1]
        )

    def get_ids_from_names(self, list_of_names: List) -> List[int]:
        """
        Retrieves the IDs of deliverables with the specified names.

        Args:
            list_of_names (list): A list of deliverable names.

        Returns:
            list: The IDs of the deliverables.

        """
        deliverables_list = self.get_deliverables_list()
        ids = []
        for id, deliverable in enumerate(deliverables_list):
            if deliverable["Name"] in set(list_of_names):
                ids.append(id)
        return ids

    def request_deliverable(self, id: int) -> requests.Response:
        """
        Requests a deliverable with the specified ID.

        Args:
            id (int): The ID of the deliverable.

        Returns:
            resquests.Response: The requested deliverable.

        """
        url = self.get_deliverable_path(id)
        return requests.get(url=url, auth=self.cred)

    def download_deliverable(self, id: int, output_dir: Path = None):
        """
        Downloads a deliverable with the specified ID and writes to output Path

        Args:
            id (int): The ID of the deliverable.
            output_dir (Path, optional): The output directory for the downloaded deliverable.
                If not specified, the deliverable will be saved in the current directory.

        Returns:
            None

        """
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

    def transfer_to_azure(
        self, url: str, name: str, session: requests.Session, blob_storage: AzureBlob
    ):
        """
        Transfers a deliverable to Azure Blob Storage.

        Args:
            url (str): The URL of the deliverable.
            name (str): The name of the deliverable.
            session (requests.Session): The session for making HTTP requests.
            blob_storage (AzureBlob): The Azure Blob Storage instance.

        Returns:
            list: A list containing the name of the deliverable and the transfer status.

        """
        try:
            response = session.get(url=url, auth=self.cred)
            content = response.content
        except Exception as e:
            logger.error(f"Could not retrieve {name}: {e}")
            return [name, "Sharepoint failure"]
        try:
            blob_storage.upload_content(name=name, content=content)
        except Exception as e:
            logger.error(f"Unable to upload {name} to Azure: {e}")
            return [name, "Azure failure"]
        logger.info(f"Uploaded {name} to Azure")
        return [name, "Success"]

    # Useful reference: https://medium.com/@angry_programmer/fast-request-using-asyncio-with-ntlm-in-python-eecf2981e9c0
    async def transfer_multiple(self, ids: list, blob_storage: AzureBlob):
        """
        Transfers multiple deliverables to Azure Blob Storage asynchronously.

        Args:
            ids (list): A list of deliverable IDs.
            blob_storage (AzureBlob): The Azure Blob Storage instance.

        Returns:
            list: A list containing the names of the deliverables and their transfer statuses.

        """
        loop = asyncio.get_event_loop()

        with requests.Session() as session:
            session.auth = self.cred
            tasks = []
            deliverables_list = self.get_deliverables_list()
            for id in ids:
                url = self.get_deliverable_path(id)
                content_name = deliverables_list[id]["Name"]
                tasks.append(
                    loop.run_in_executor(
                        None,
                        self.transfer_to_azure,
                        url,
                        content_name,
                        session,
                        blob_storage,
                    )
                )

            results = await asyncio.gather(*tasks)
            return results

    def update_azure(self, azure_blob: AzureBlob):
        """
        Updates Azure Blob Storage with the latest deliverables from SharePoint.

        Args:
            azure_blob (AzureBlob): The Azure Blob Storage instance.

        Returns:
            None

        """
        deliverables = self.get_deliverables_name_list()
        already_transferred = azure_blob.get_uploaded_deliverables()
        to_be_transferred = list(set(deliverables) - set(already_transferred))
        ids = self.get_ids_from_names(to_be_transferred)
        asyncio.new_event_loop().run_until_complete(
            self.transfer_multiple(ids=ids, blob_storage=azure_blob)
        )


def main():
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

    deliverables_names = eurokin.get_deliverables_name_list()
    transfer_test = deliverables_names[:10]
    ids = eurokin.get_ids_from_names(transfer_test)

    azure_secrets = secrets["azure"]
    eurokin_azure = AzureBlob(secrets=azure_secrets)

    eurokin.transfer_multiple(ids=ids, blob_storage=eurokin_azure)


if __name__ == "__main__":
    main()
