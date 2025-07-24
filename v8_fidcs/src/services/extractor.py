from v8_utilities.paths import PathV8
from v8_utilities.sharepoint import SharePoint
from v8_utilities.anbima_calendar import Calendar
from v8_fidcs.src.others.logger import LogFIDC
from typing import List, Optional

import pandas as pd

import requests
import datetime
import os

logger = LogFIDC()

class Extractor(SharePoint):
    def __init__(self, path_handle: PathV8, calendar_handle: Calendar, site_name: str, folder_root:str = None) -> None:
        #super().__init__(tenant_id, client_id, client_secret, authority_url, site_domain, site_name, site_id)
        super().__init__(site_name)
        self.calendar_handle = calendar_handle
        self.path_handle = path_handle

        if folder_root is None:
            self.folder_root = self.path_handle.FIDCS_RELATORIOS_GERAIS
        else:
            self.folder_root = folder_root

    def _build_path(self, date: datetime.date) -> str:
        """
        Constrói o caminho da pasta de download dos arquivos Excel,
        organizados por ano e mês, baseado na data fornecida.

        Args:
            date (datetime.date): Data usada para determinar o ano e o mês do caminho.

        Returns:
            str: Caminho relativo formatado para a pasta de download, com separadores de diretório ajustados para "/".

        Raises:
            Exception: Relança qualquer exceção ocorrida durante a construção do caminho, após registrar o erro.
        """
        try:
            nome_mes = self.calendar_handle.get_month_by_number(date.month)

            return os.path.join(
                "FIDCs Investidos",
                "Relatórios",
                "Planilhas de Monitoramentos",
                str(date.year),
                f"Relatórios {nome_mes.capitalize()}"
            ).replace("\\", "/")

        except Exception as e:
            logger.error(f"Erro ao construir caminho de download: {e}")
            raise e

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
            logger.error(f"Timeout ao listar arquivos na pasta '{path_file}'.")
            raise(TimeoutError(f"Timeout ao listar arquivos na pasta '{path_file}'."))
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de requisição ao listar arquivos na pasta '{path_file}': {e}")
            raise(requests.exceptions.RequestException(f"Erro de requisição ao listar arquivos na pasta '{path_file}': {e}"))
        except Exception as e:
            logger.error(f"Erro inesperado ao listar arquivos na pasta '{path_file}': {e}")
            raise(Exception(f"Erro inesperado ao listar arquivos na pasta '{path_file}': {e}"))

    def list_fidcs(self, date: datetime.date) -> Optional[List[str]]:
        """
        Lista as pastas ou arquivos presentes no caminho construído a partir da data fornecida.

        Args:
            date (datetime.date): Data usada para determinar a pasta onde a listagem será feita.

        Returns:
            Optional[List[str]]: Lista com os nomes das pastas/arquivos encontrados. Retorna None em caso de erro.

        Notes:
            Caso ocorra algum erro ao listar os arquivos, um erro é registrado e a função retorna None.
        """
        try:
            path_to_download = self._build_path(date)
            fidc_list = self.list_files(path_to_download)
            return fidc_list

        except Exception as e:
            logger.error(f"Erro ao listar os FIDCs.")
            return None

    def download_fidcs(self, date: datetime.date, fidc_list: List[str]) -> List[str]:
        """
        Realiza o download dos arquivos .xlsx correspondentes à lista de FIDCs para a data especificada,
        salvando-os na pasta "00_RAW" dentro do diretório raiz configurado.

        Args:
            fidc_list (List[str]): Lista com os nomes das pastas/arquivos FIDC a serem baixados.
            date (datetime.date): Data usada para construir o caminho de origem e nomear os arquivos baixados.

        Returns:
            List[str]: Lista atualizada dos FIDCs que foram baixados com sucesso. FIDCs com falha no download são removidos da lista.

        Raises:
            Exception: Relança qualquer exceção inesperada ocorrida durante o processo de download geral após registrar o erro.
        """
        try:
            raw_path = os.path.join(self.folder_root, "00_RAW")
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