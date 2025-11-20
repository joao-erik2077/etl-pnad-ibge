import logging
import pandas as pd
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError

def extract_dataframe(project_id: str, query: str) -> pd.DataFrame:
    logging.info("Iniciando extração da PNAD (BigQuery)")

    try:
        client = bigquery.Client(project=project_id)
        logging.info(f"Client BigQuery criado com sucesso (projeto: {project_id})")

    except DefaultCredentialsError:
        logging.error("❌ Falha ao criar client BigQuery: credenciais não encontradas.")
        _print_auth_help()
        raise SystemExit("Encerrando ETL por falta de credenciais.")

    except Exception as e:
        logging.error(f"❌ Erro inesperado ao criar o client BigQuery: {e}")
        raise

    logging.info("Executando query no BigQuery...")

    try:
        query_job = client.query(query)
        df = query_job.to_dataframe()
        logging.info(f"Extração concluída: {len(df)} registros recebidos.")
        return df

    except Exception as e:
        logging.error("❌ Erro ao executar a query no BigQuery.")
        logging.error(f"Detalhes: {e}")
        raise