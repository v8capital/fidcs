from v8_utilities.paths import PathV8
from v8_utilities.anbima_calendar import Calendar

from v8_fidcs.src.parser.exceltransformer import ExcelTransformer
from v8_fidcs.src.others.logger import LogFIDC

from typing import List

import os

logger = LogFIDC()

class Transformer(object):
    def __init__(self, path_handle: PathV8, calendar_handle: Calendar, folder_root: str = None):
        self.path_handle = path_handle
        self.calendar_handle = calendar_handle

        self.fidc_renames = {"ONIXOLD": "ONIX", "IOSAN": "IOXI(IOSAN)", "OXSS": "IOXII(OXSS)", "IOXII": "IOXII(OXSS)"}

        if folder_root is None:
            self.folder_root = self.path_handle.FIDCS_RELATORIOS_GERAIS
        else:
            self.folder_root = folder_root

    def run(self, date: str, fidc_list: List[str]) -> List[str]:
        """
        Processa e transforma os arquivos Excel de uma lista de FIDCs, salvando-os como CSVs no caminho destino.

        Para cada FIDC na lista:
            - Constrói o caminho do arquivo Excel de entrada.
            - Constrói o caminho do arquivo CSV de saída.
            - Cria diretórios necessários para salvar o CSV.
            - Executa a transformação via ExcelTransformer.
            - Registra sucesso ou falha, removendo da lista os que falharem.

        Args:
            date (str): Data no formato YYYY_MM_DD
            fidc_list (List[str]): Lista de nomes dos FIDCs a serem processados.

        Returns:
            List[str]: Lista atualizada de FIDCs que foram processados com sucesso.
        """
        try:
            for fidc_name in fidc_list[:]:
                try:
                    file_name_read = f"FIDC_{fidc_name}_" + date + ".xlsx"
                    path_target_r = os.path.join(self.folder_root, "00_RAW", file_name_read)

                    fidc_name_updated = self.fidc_renames.get(fidc_name, fidc_name)
                    file_name_save = f"FIDC_{fidc_name_updated}_" + date + ".csv"
                    path_target_s = os.path.join(self.folder_root, "01_PARSED", file_name_save)

                    os.makedirs(os.path.dirname(path_target_s), exist_ok=True)

                    ExcelTransformer(self.path_handle, self.calendar_handle, path_target_r, path_target_s, fidc_name).transform_table()

                    logger.info(f"O FIDC {fidc_name} foi tratado com sucesso.")
                except Exception as e:
                    fidc_list.remove(fidc_name)
                    logger.error(f"O FIDC {fidc_name} não foi tratado, devido ao erro: {e}")
            fidc_list = [self.fidc_renames.get(fidc, fidc) for fidc in fidc_list]
            return fidc_list
        except Exception as e:
            logger.error(f"Transformação dos Dados para o Mês {date}: {e}")
            raise e