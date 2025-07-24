import pandas as pd

from v8_fidcs.src.services.extractor import Extractor
from v8_fidcs.src.services.transformer import Transformer
from v8_fidcs.src.services.grouper import Grouper
from v8_fidcs.src.others.logger import LogFIDC
from v8_utilities.paths import PathV8
from v8_utilities.anbima_calendar import Calendar

import os
import datetime

#parsed_path = os.path.join(folder_root, "01_PARSED")
#grouped_path = os.path.join(folder_root, "02_GROUPED")

logger = LogFIDC()

def extract(path_handle, calendar_handle, date, folder_root = None):
    try:
        logger.info(f"Iniciando Processo de Extração dos Dados para o Mês {date}.")

        extr = Extractor(path_handle, calendar_handle, "FIDCS")
        fidc_list = extr.list_fidcs(date)

        fidc_list = extr.download_fidcs(date, fidc_list)

        logger.info(f"O Processo de Extração dos Dados para o Mês {date}"
                    f"foi concluído com sucesso.")

        return fidc_list

    except Exception as e:
        logger.error(f"O Processo de Extração para o mês {date} "
                     f"não foi concluído corretamente devido ao erro: {e}.")

        return []

def transform(path_handle, calendar_handle, date, fidc_list, folder_root = None):
    try:
        logger.info(f"Iniciando Processo de Tratamento dos Dados para o Mês {date}.")

        date_str = date.strftime("%Y_%m_%d")

        transf = Transformer(path_handle, calendar_handle)
        fidc_list = transf.run(date_str, fidc_list)

        logger.info(f"O Processo de Tratamento dos Dados para o Mês {date}"
                    f" foi concluído com sucesso.")

        return fidc_list

    except Exception as e:
        logger.error(f"O Processo de Tratamento para o mês {date}"
                     f" não foi concluído corretamente devido ao erro: {e}.")

        return []

def group(path_handle, calendar_handle, date, fidc_list, folder_root = None):
    try:
        logger.info(f"Iniciando Processo de Agrupamento dos Dados para o Mês {date}.")

        grouper = Grouper(path_handle, calendar_handle)
        result = grouper.run(date)

        logger.info(f"O Processo de Agrupamento dos Dados para o Mês {date}"
                    f" foi concluído com sucesso.")

        return result
    except Exception as e:
        logger.error(f"O Processo de Agrupamento para o mês {date} "
                     f"não foi concluído corretamente devido ao erro: {e}.")

        return 0

path_handle = PathV8()
calendar_handle = Calendar()
# date = calendar_handle.get_start_of_month(calendar_handle.today())
date = calendar_handle.get_start_of_month(datetime.date(2025, 5, 22),
                                                  only_workdays=False)
folder_root = os.path.abspath(os.path.join(os.getcwd(), "data"))

fidc_list = extract(path_handle, calendar_handle, date, folder_root)
fidc_list = transform(path_handle, calendar_handle, date, fidc_list, folder_root)
result = group(path_handle, calendar_handle, date, fidc_list, folder_root)
