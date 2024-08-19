from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
import logging
from pathlib import Path
import json

logger = logging.getLogger(__name__)


class AzureBlob:
    """
    Represents a client for interacting with Azure Blob Storage.
    """

    # TODO: This should probably just inherit BlobServiceClient directly

    def __init__(self, secrets: dict) -> BlobServiceClient:
        """
        Initializes a new instance of the AzureBlob class.

        Args:
            secrets (dict): A dictionary containing the connection string.

        Returns:
            self: An instance of the AzureBlob class.
        """
        if {"connect_string"} <= set(secrets):
            self.connect_string = secrets["connect_string"]
        else:
            raise KeyError("Secrets dict must contain connect_string")
        self.service_client = BlobServiceClient.from_connection_string(
            self.connect_string
        )
        self.deliverables = None
        return None

    def get_deliverables_container(self):
        """
        Gets the deliverables container from Azure Blob Storage.
        If the container does not exist, it creates a new one.

        Returns:
            None
        """
        if self.deliverables is not None:
            pass
        else:
            container_list = [
                container.name for container in self.service_client.list_containers()
            ]
            if "deliverables" in set(container_list):
                self.deliverables = self.service_client.get_container_client(
                    "deliverables"
                )
            else:
                self.deliverables = self.service_client.create_container("deliverables")

    def upload_file(self, file_path: Path):
        """
        Uploads a file to the deliverables container in Azure Blob Storage.

        Args:
            file_path (Path): The path to the file to be uploaded.

        Returns:
            None
        """
        self.get_deliverables_container()
        blob_client = self.deliverables.get_blob_client(blob=file_path.name)
        try:
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data)
            logger.info(file_path.name + " uploaded successfully")
        except ResourceExistsError:
            logger.info(file_path.name + " already uploaded, skipping")
        except Exception as e:
            logger.error(f"{file_path.name} not uploaded, exception {e}")
            pass

    def upload_content(self, name: str, content: bytes):
        """
        Uploads content to the deliverables container in Azure Blob Storage.

        Args:
            name (str): The name of the blob.
            content (bytes): The content to be uploaded.

        Returns:
            None
        """
        self.get_deliverables_container()
        blob_client = self.deliverables.get_blob_client(blob=name)
        try:
            blob_client.upload_blob(content)
        except ResourceExistsError:
            logger.info(name + " already uploaded, skipping")
            pass

    def upload_content_stream(self, name: str, content_stream):
        """
        Uploads content to the deliverables container in Azure Blob Storage.

        Args:
            name (str): The name of the blob.
            content_stream (io.BytesIO): The content to be uploaded.

        Returns:
            None
        """
        # https://learn.microsoft.com/en-us/dotnet/api/azure.storage.blobs.specialized.blockblobclient?view=azure-dotnet
        # Need to re-write for large files using streaming (I think)
        pass

    def get_uploaded_deliverables(self):
        """
        Gets the list of uploaded deliverables from the deliverables container.

        Returns:
            List[str]: A list of blob names.
        """
        self.get_deliverables_container()
        return [name for name in self.deliverables.list_blob_names()]


if __name__ == "__main__":
    cwd = Path().cwd()
    deliverables_dir = cwd / "Deliverables"

    with open("secrets.json") as secrets_file:
        secrets = json.load(secrets_file)
    azure_secrets = secrets["azure"]

    eurokin_azure = AzureBlob(azure_secrets)

    transferred_items = eurokin_azure.get_uploaded_deliverables()

    assert False
