from v8_fidcs.src.services.extractor import Extractor
from v8_fidcs.src.services.transformer import Transformer
from v8_fidcs.src.services.grouper import Grouper
from v8_fidcs.src.others.logger import LogFIDC
from utilities.v8_utilities.paths import PathV8
from utilities.v8_utilities.anbima_calendar import Calendar

import os
import datetime

#parsed_path = os.path.join(folder_root, "01_PARSED")
#grouped_path = os.path.join(folder_root, "02_GROUPED")

logger = LogFIDC()

def extract(path_handle, calendar_handle, date, folder_root):
    try:
        logger.info(f"Iniciando Processo de Extração dos Dados para o Mês {date}.")

        extr = Extractor("FIDCS")
        fidc_list = extr.list_fidcs(date)

        # tem q ver isso

        fidc_list = extr.download_fidcs(fidc_list, date, folder_root)

        logger.info(f"O Processo de Extração dos Dados para o Mês {date}"
                    f"foi concluído com sucesso.")

        return fidc_list

    except Exception as e:
        logger.error(f"O Processo de Extração para o mês {date} "
                     f"não foi concluído corretamente devido ao erro: {e}.")

        return []

def transform(path_handle, calendar_handle, date, folder_root, fidc_list):
    try:
        logger.info(f"Iniciando Processo de Tratamento dos Dados para o Mês {date}.")

        raw_path = os.path.join(folder_root, "00_RAW")
        parsed_path = os.path.join(folder_root, "01_PARSED")
        date_str = date.strftime("%Y_%m_%d")

        transf = Transformer(date_str)
        fidc_list = transf.run(raw_path, parsed_path, fidc_list)

        logger.info(f"O Processo de Tratamento dos Dados para o Mês {date}"
                    f" foi concluído com sucesso.")

        return fidc_list


    except Exception as e:
        logger.error(f"O Processo de Tratamento para o mês {date}"
                     f" não foi concluído corretamente devido ao erro: {e}.")

        return []

def group(path_handle, calendar_handle, date, folder_root):
    try:
        logger.info(f"Iniciando Processo de Agrupamento dos Dados para o Mês {date}.")

        parsed_path = os.path.join(folder_root, "01_PARSED")
        grouped_path = os.path.join(folder_root, "02_GROUPED")
        date_str = date.strftime("%Y-%m-%d")

        transf = Grouper(date)
        result = transf.run(parsed_path, grouped_path)

        #dinamizar leitura para o novo formato com a data
        #ajeitar salvamento por meio do recebimento do path
        #padronizar envio da data

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
fidc_list = transform(path_handle, calendar_handle, date, folder_root, fidc_list)
result = group(path_handle, calendar_handle, date, folder_root)

print(result)