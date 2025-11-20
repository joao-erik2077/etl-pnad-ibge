import logging
import pandas as pd

def transform_pnad_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transformando dados da PNAD")

    cols = set(df.columns)
    logging.info(f"Colunas recebidas do BigQuery: {sorted(cols)}")
    logging.info(f"Transformação concluída. Total de linhas antes da limpeza: {len(df)}")

    # Filter required columns
    required_columns = []
    for col in ["renda_domiciliar", "moradores"]:
        if col not in cols:
            raise KeyError(f"Coluna obrigatória '{col}' não encontrada no DataFrame.")
        required_columns.append(col)
    

    # Drop NaN in required columns
    df = df.dropna(subset=required_columns)

    # Filter negative values
    if "renda_total" in cols:
        df = df[df["renda_total"].fillna(0) >= 0]

    # Create 'Renda per Capita' column
    df = df[df["moradores"] > 0]
    df["renda_pc"] = df["renda_domiciliar"] / df["moradores"]

    # Normalization of the "sexo" field
    if "sexo" in cols:
        try:
            df["sexo"] = df["sexo"].astype("Int64").replace({1: "M", 2: "F"})
        except Exception:
            df["sexo"] = df["sexo"].astype(str).str.upper()

    # Create 'Faixa Etária' column
    if "idade" in cols:
        df["faixa_etaria"] = pd.cut(
            df["idade"],
            bins=[0, 18, 30, 45, 60, 200],
            labels=["0-18", "19-30", "31-45", "46-60", "60+"]
        )
    
    # Create 'Região' column
    uf_col = None
    if "uf" in cols:
        uf_col = "uf"
    elif "sigla_uf" in cols:
        uf_col = "sigla_uf"
        df = df.rename(columns={"sigla_uf": "uf"})
        uf_col = "uf"

    if uf_col:
        REGIOES = {
            "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte",
            "RO": "Norte", "RR": "Norte", "TO": "Norte",
            "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste",
            "MA": "Nordeste", "PB": "Nordeste", "PE": "Nordeste",
            "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
            "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
            "PR": "Sul", "RS": "Sul", "SC": "Sul",
            "DF": "Centro-Oeste", "GO": "Centro-Oeste",
            "MS": "Centro-Oeste", "MT": "Centro-Oeste",
        }
        df["regiao"] = df["uf"].map(REGIOES)
    else:
        logging.warning("Nenhuma coluna de UF encontrada ('uf' ou 'sigla_uf'). Região não será criada.")

    logging.info(f"Transformação concluída. Total de linhas após limpeza: {len(df)}")
    return df