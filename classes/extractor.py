from v8_utilities.sharepoint import SharePoint
from classes.logger import LogFIDC

from typing import List

# não vou ler os YAMLs aqui não, talvez mudar isso nos outros depois

logger = LogFIDC()

class Extractor(SharePoint):
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, authority_url: str, site_domain: str, site_name: str, site_id: str) -> None:
        super().__init__(tenant_id, client_id, client_secret, authority_url, site_domain, site_name, site_id)

    # depois ver melhor como vai ser esse PATH
    def extract_fidcs(self, path: str, fidcs_list: List[str]) -> str:
        """
        Extrai arquivos FIDC do SharePoint e salva localmente.

        Parâmetros:
            path (str): Caminho do arquivo no SharePoint.
            fidcs_list (List[str]): Lista de nomes dos FIDCs a serem extraídos.

        Retorna:
            str: Caminho do arquivo baixado.
        """
        for fidc in fidcs_list:
            try:
                file_name = f"FIDC_{fidc}_2025_04_30.xlsx"
                local_path = f"{path}/{file_name}"
                self.download_file(path, file_name, local_path)
                logger.info(f"Arquivo {file_name} baixado com sucesso.")
                return local_path
            except Exception as e:
                logger.error(f"Erro ao baixar o arquivo {fidc}: {e}")

