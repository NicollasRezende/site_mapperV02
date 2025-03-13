import os
import csv
from datetime import datetime
from typing import List

class FileUtils:
    @staticmethod
    def init_csv(filename: str, headers: List[str]) -> None:
        """Inicializa arquivo CSV com cabeçalho."""
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)

    @staticmethod
    def append_to_csv(filename: str, row: List[str]) -> None:
        """Adiciona uma linha ao arquivo CSV."""
        with open(filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(row)

    @staticmethod
    def generate_timestamp() -> str:
        """Gera timestamp formatado para nomes de arquivo."""
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    @staticmethod
    def ensure_directory(directory: str) -> None:
        """Garante que um diretório existe."""
        os.makedirs(directory, exist_ok=True)