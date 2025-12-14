import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime


class DatabaseExtractor:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)

    def extract_table_to_csv(self, table_name, output_folder):
        """
        Extrai uma tabela inteira do banco e salva na camada BRONZE.
        """
        print(f"--- [ETL] Iniciando extração da tabela: {table_name} ---")

        # 1. Ler do Banco (SQL)
        query = f"SELECT * FROM {table_name}"
        try:
            df = pd.read_sql(query, self.engine)
            print(f"[OK] Dados extraídos. Linhas: {len(df)}")
        except Exception as e:
            print(f"[ERRO] Falha ao conectar no banco: {e}")
            return

        # 2. Gerar Nome do Arquivo com Timestamp (Versionamento de Dado)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_{timestamp}.csv"

        # Cria a pasta se não existir
        os.makedirs(output_folder, exist_ok=True)
        file_path = os.path.join(output_folder, filename)

        # 3. Salvar na Bronze (Raw)
        df.to_csv(file_path, index=False)
        print(f"[OK] Arquivo salvo em: {file_path}")


# --- Exemplo de Uso (main.py) ---
if __name__ == "__main__":
    # Exemplo de Connection String (Postgres)
    DB_URL = "postgresql://user:pass@localhost:5432/ortzion_db"

    extractor = DatabaseExtractor(DB_URL)

    # Extrai para a camada Bronze
    extractor.extract_table_to_csv(
        table_name="transacoes_financeiras",
        output_folder="datalake/1_bronze/financeiro"
    )