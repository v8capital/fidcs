from v8_fidcs.src.parser.exceltransformer import ExcelTransformer
from v8_fidcs.src.others.logger import LogFIDC

import os

logger = LogFIDC()

class Transformer(object):
    def __init__(self, date_str):
        self.date = date_str
    def run(self, path_to_read, path_to_save, fidc_list):
        try:
            for fidc_name in fidc_list[:]:
                try:
                    file_name_read = f"FIDC_{fidc_name}_" + self.date + ".xlsx"
                    path_target_r = os.path.join(path_to_read, file_name_read)

                    file_name_save = f"FIDC_{fidc_name}_" + self.date + ".csv"
                    path_target_s = os.path.join(path_to_save, file_name_save)

                    os.makedirs(os.path.dirname(path_target_s), exist_ok=True)

                    ExcelTransformer(path_target_r, fidc_name).transform_table(path_target_s)

                    logger.info(f"O FIDC {fidc_name} foi tratado com sucesso.")
                except Exception as e:
                    fidc_list.remove(fidc_name)
                    logger.error(f"O FIDC {fidc_name} não foi tratado, devido ao erro: {e}")
            return fidc_list
        except Exception as e:
            logger.error(f"Transformação dos Dados para o Mês {self.date}: {e}")
            raise e