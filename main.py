import logging
from src.extract.search_query import SearchQueryBuilder
from src.database.url import get_database_url

POSTGRES_URL = get_database_url()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ETL] %(message)s")

def build_pnad_query() -> str:
    query = SearchQueryBuilder("basedosdados.br_ibge_pnadc.microdados")

    query.add_column("ano")
    query.add_column_with_alias("sigla_uf", "uf")
    query.add_column_with_alias("V2007", "sexo")
    query.add_column_with_alias("V2009", "idade")
    query.add_column_with_alias("VD4019", "renda_domiciliar")
    query.add_column_with_alias("V2001", "moradores")
    query.add_column_with_alias("V1027", "peso")

    query.column_greater_than_or_equal("ano", 2022)
    
    query.order_by("ano")

    query.add_limit(1000)
    
    return query.get_query()

def run():
    build_pnad_query()

if __name__ == "__main__":
    run()