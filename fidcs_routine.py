import pandas as pd

from v8_fidcs.src.services.extractor import Extractor
from v8_fidcs.src.services.transformer import Transformer
from v8_fidcs.src.services.grouper import Grouper
from v8_fidcs.src.others.logger import LogFIDC
from v8_utilities.paths import PathV8
from v8_utilities.anbima_calendar import Calendar

import os
import datetime

# parsed_path = os.path.join(folder_root, "01_PARSED")
# grouped_path = os.path.join(folder_root, "02_GROUPED")

logger = LogFIDC()


def extract(path_handle, calendar_handle, date, fidc_list, folder_root=None):
    try:
        logger.info(f"Iniciando Processo de Extração dos Dados para o Mês {date}.")

        extr = Extractor(path_handle, calendar_handle, "FIDCS", folder_root)

        if not fidc_list:
            fidc_list = extr.list_fidcs(date)

        logger.info(f"FIDCS que devem ser baixados: {fidc_list}")
        if not fidc_list:
            raise ValueError("Nenhum FIDC encontrado na listagem inicial.")
        fidc_list_downloaded = extr.download_fidcs(date, fidc_list)

        if not fidc_list_downloaded:
            logger.error(f"Erro total na extração: nenhum FIDC foi baixado.")
            return []

        if len(fidc_list_downloaded) == len(fidc_list):
            logger.info(f"O Processo de Extração dos Dados para o mês {date} concluída com 100% de sucesso.")
            return fidc_list_downloaded
        else:
            faltantes = set(fidc_list) - set(fidc_list_downloaded)
            percentual = 100 * (len(faltantes) / len(fidc_list))
            logger.warning(f"Extração parcialmente concluída. "
                           f"{percentual:.1f}% dos FIDCs falharam: {faltantes}")
            return fidc_list_downloaded

    except Exception as e:
        logger.error(f"Erro total na extração: {e}")
        return []


def transform(path_handle, calendar_handle, date, fidc_list, folder_root=None):
    try:
        logger.info(f"Iniciando Processo de Tratamento dos Dados para o Mês {date}.")
        logger.info(f"FIDCS que devem ser transformados: {fidc_list}")

        date_str = date.strftime("%Y_%m_%d")

        transf = Transformer(path_handle, calendar_handle, folder_root)
        fidc_list_transformed = transf.run(date_str, fidc_list)

        if not fidc_list_transformed:
            logger.error("Erro total no tratamento: lista final vazia.")
            return []

        if len(fidc_list_transformed) == len(fidc_list):
            logger.info("Tratamento concluído com 100% de sucesso.")
            return fidc_list_transformed
        else:
            faltantes = set(fidc_list) - set(fidc_list_transformed)
            percentual = 100 * (len(faltantes) / len(fidc_list))
            logger.warning(f"Tratamento parcialmente concluído. "
                           f"{percentual:.1f}% dos FIDCs falharam: {faltantes}")
            return fidc_list_transformed


    except Exception as e:
        logger.error(f"Erro total no tratamento: {e}")

        return []


def group(path_handle, calendar_handle, date, fidc_list, folder_root=None):
    try:
        logger.info(f"Iniciando Processo de Agrupamento dos Dados para o Mês {date}.")
        logger.info(f"FIDCS que devem ser agrupados: {fidc_list}")

        grouper = Grouper(path_handle, calendar_handle, folder_root)
        fidc_list_grouped = grouper.run(date)

        if not fidc_list_grouped:
            logger.error("Erro total no agrupamento: lista final vazia.")
            return []

        if len(fidc_list_grouped) == len(fidc_list):
            logger.info("Agrupamento concluído com 100% de sucesso.")
            return fidc_list_grouped
        else:
            faltantes = set(fidc_list) - set(fidc_list_grouped)
            percentual = 100 * (len(faltantes) / len(fidc_list))
            logger.warning(f"Agrupamento parcialmente concluído. "
                           f"{percentual:.1f}% dos FIDCs falharam: {faltantes}")
            return fidc_list_grouped
    except Exception as e:
        logger.error(f"Erro total no agrupamento: {e}")
        return []