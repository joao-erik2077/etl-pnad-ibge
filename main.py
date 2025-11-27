import logging
from src.database.url import get_database_url
from src.database.database_loader import load_to_database
from src.extract.search_query_builder import SearchQueryBuilder
from src.extract.bigquery_extractor import extract_dataframe
from src.transform.pnad_transform import transform_pnad_dataframe

POSTGRES_URL = get_database_url()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ETL] %(message)s")

def build_pnad_query() -> str:
    query = (
        SearchQueryBuilder(table_name="basedosdados.br_ibge_pnadc.microdados")
        .add_column(column_name="ano")
        .add_column_with_alias(column_name="sigla_uf", alias="uf")
        .add_column_with_alias(column_name="V2007", alias="sexo")
        .add_column_with_alias(column_name="V2009", alias="idade")
        .add_column_with_alias(column_name="VD4019", alias="renda_domiciliar")
        .add_column_with_alias(column_name="V2001", alias="moradores")
        .add_column_with_alias(column_name="V1027", alias="peso")
        .add_column_with_alias(column_name="V3009A", alias="maior_curso_frequentado")
        .add_column_with_alias(column_name="V3008", alias="frequentou_escola")
        .add_column_with_alias(column_name="V3001", alias="sabe_ler_escrever")
        .add_column_with_alias(column_name="V3002A", alias="rede_ensino")

        .column_greater_than_or_equal(column_name="VD4019", value=0)
        .column_greater_than_or_equal(column_name="ano", value=2022)
        .columns_are_not_null(columns=["V3009A", "V3002A"])
        
        .order_by(column_name="ano")

        .add_limit(limit_value=1000000)
    )
    return query.get_query()

def run():
    query = build_pnad_query()
    df = extract_dataframe(project_id="etl-pnad", query=query)
    df = transform_pnad_dataframe(df)
    load_to_database(df, "pnad_tratada", POSTGRES_URL)

if __name__ == "__main__":
    run()