import logging
from sqlalchemy import create_engine
import pandas as pd

def load_to_database(df: pd.DataFrame, table_name: str, conn_str: str):
    logging.info(f"Carregando dados em {table_name}...")
    engine = create_engine(conn_str)
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    logging.info("Carga concluída.")