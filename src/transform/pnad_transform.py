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
    
    # Sabe ler e escrever
    if "sabe_ler_escrever" in df.columns:
        df["V3001_norm"] = (
            df["sabe_ler_escrever"]
            .astype(str)
            .str.extract(r"(\d+)")[0]   # pega somente o número
        )

        df["sabe_ler_escrever"] = df["V3001_norm"].map({
            "1": "Sabe ler e escrever",
            "2": "Não sabe ler e escrever"
        }).fillna("Não informado")

    # Frequentou escola
    if "frequentou_escola" in df.columns:
        df["V3008_norm"] = (
            df["frequentou_escola"]
            .astype(str)
            .str.extract(r"(\d+)")[0]
        )

        df["frequentou_escola"] = df["V3008_norm"].map({
            "1": "Sim, já frequentou",
            "2": "Nunca frequentou"
        }).fillna("Não informado")


    # Rede pública ou privada
    if "rede_ensino" in df.columns:
        df["V3002A_norm"] = (
            df["rede_ensino"]
            .astype(str)
            .str.extract(r"(\d+)")[0]
        )

        df["rede_ensino"] = df["V3002A_norm"].map({
            "1": "Privada",
            "2": "Pública"
        }).fillna("Não informado")


    # Maior curso que frequentou
    if "maior_curso_frequentado" in df.columns:
        df["V3009_norm"] = (
            df["maior_curso_frequentado"]
            .astype(str)
            .str.extract(r"(\d+)")[0]
        )

        df["maior_curso_frequentado"] = df["V3009_norm"].map({
            "1": "Creche",
            "2": "Pré-escola",
            "3": "AJA - Alfabetização de jovens e adultos",
            "4": "Classe de alfabetização (CA)",
            "5": "Ensino Fundamental 1 (1ª a 4ª série)",
            "6": "Ensino Fundamental 2 (5ª a 8ª série)",
            "7": "EJA - Ensino Fundamental",
            "8": "Ensino Médio (regular)",
            "9": "EJA - Ensino Médio",
            "10": "Técnico de nível médio",
            "11": "Superior - graduação",
            "12": "Especialização (pós-graduação lato sensu)",
            "13": "Mestrado",
            "14": "Doutorado"
        }).fillna("Não informado")


    logging.info(f"Transformação concluída. Total de linhas após limpeza: {len(df)}")
    return df