from utilities.v8_utilities.logv8 import LogV8
from datetime import date
from pathlib import Path

import logging

class LogFIDC(LogV8):
    """
    Classe de log para FIDCs, herda de LogV8.
    Configura o logger com nível DEBUG e formatação específica.
    """

    def __init__(self, level = logging.INFO, log_path="Default"):
        if log_path == "Default":
            log_path = self._create_log_path()
        super().__init__(level=level, log_path=log_path)

    def _create_log_path(self):
        """
        Cria o caminho do log com base no nome do módulo.
        Se o caminho for 'Default', usa o diretório padrão.
        """
        today_str = date.today().strftime("%Y-%m-%d")
        log_dir   = Path("./logs")
        log_dir.mkdir(exist_ok=True)
        log_path = log_dir / f"LOG_{today_str}"
        return log_path