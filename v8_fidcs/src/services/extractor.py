from utilities.v8_utilities.sharepoint import SharePoint
from v8_fidcs.src.others.logger import LogFIDC
from typing import List, Optional

import pandas as pd

import requests
import datetime
import os

logger = LogFIDC()

class Extractor(SharePoint):
    def __init__(self, site_name) -> None:
        #super().__init__(tenant_id, client_id, client_secret, authority_url, site_domain, site_name, site_id)
        super().__init__(site_name)

    def _build_path(self, date: datetime.date) -> str:
        try:
            meses_pt = [
                "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
            ]
            nome_mes = meses_pt[date.month - 1] # ver dps
            return os.path.join(
                "FIDCs Investidos",
                "Relatórios",
                "Planilhas de Monitoramentos",
                str(date.year),
                f"Relatórios {nome_mes}"
            ).replace("\\", "/")
        except Exception as e:
            logger.error(f"Erro ao construir caminho de download: {e}")
            raise e

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


    def list_fidcs(self, date: datetime.date) -> Optional[List[str]]:
        try:
            path_to_download = self._build_path(date)
            fidc_list = self.list_files(path_to_download)
            return fidc_list

        except Exception as e:
            logger.error(f"Erro ao listar os FIDCs.")
            return None


    def download_fidcs(self, fidc_list: list[str], date: datetime.date, folder_root: str):
        try:
            raw_path = os.path.join(folder_root, "00_RAW")
            date_str = date.strftime("%Y_%m_%d")


            path_to_download = self._build_path(date)
            for fidc_name in fidc_list[:]:
                try:
                    file_path = f"{path_to_download}/{fidc_name}"
                    file_name = f"FIDC_{fidc_name}_{date_str}.xlsx"
                    path_target = os.path.join(raw_path, file_name)

                    os.makedirs(os.path.dirname(path_target), exist_ok=True)

                    self.download_file(file_path, file_name, path_target)

                    if not os.path.exists(path_target):
                        fidc_list.remove(fidc_name)
                        logger.error(f"O Arquivo {fidc_name} não será baixado.")

                except Exception as error:
                    logger.error(f"Erro Inesperado para o FIDC {fidc_name}: {error}")

            return fidc_list
        except Exception as e:
            logger.error(f"Erro em baixar os FIDCs.")
            raise e