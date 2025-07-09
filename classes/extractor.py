from v8_utilities.sharepoint import SharePoint
from classes.logger import LogFIDC
from typing import List

import pandas as pd

import requests

# não vou ler os YAMLs aqui não, talvez mudar isso nos outros depois

logger = LogFIDC()

class Extractor(SharePoint):
    def __init__(self, site_name) -> None:
        #super().__init__(tenant_id, client_id, client_secret, authority_url, site_domain, site_name, site_id)
        super().__init__(site_name)

    # depois ver melhor como vai ser esse PATH
    def list_files(self, path_file: str) -> List[str]:
        """
        Lista os arquivos e pastas dentro de um caminho específico no SharePoint.
        Parâmetros:
            path_file (str): Caminho da pasta onde os arquivos estão localizados, separado por '/'.
        Retorna:
            List[str]: Lista de nomes de arquivos e pastas encontrados no caminho especificado.
        """
        try:
            item_id = self._get_item_id(path_file)
            if item_id is None:
                logger.error(f"Item ID não encontrado para o caminho '{path_file}'.")
                raise Exception(f"Item ID não encontrado para o caminho.")

            item_id_value = next(iter(item_id.values()))
            drive_item_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/items/{item_id_value}/children"

            response = requests.get(drive_item_url, headers=self.headers, timeout=15)
            response.raise_for_status()

            folder_names = [item["name"] for item in response.json().get('value', [])]

            # folder_names = [item['name'] for item in items]

            return folder_names
        # caso ocorra erro, aqui tem que acabar, pq é a base de tudo
        except requests.exceptions.Timeout:
            raise(TimeoutError(f"Timeout ao listar arquivos na pasta '{path_file}'."))
        except requests.exceptions.RequestException as e:
            raise(requests.exceptions.RequestException(f"Erro de requisição ao listar arquivos na pasta '{path_file}': {e}"))
        except Exception as e:
            raise(Exception(f"Erro inesperado ao listar arquivos na pasta '{path_file}': {e}"))
