import basedosdados as bd
import pandas as pd
import os
from dotenv import load_dotenv

def main():
    load_dotenv()
    billing_id = os.getenv("BILLING_ID")
    output_path = '../data/processed/dataset_municipios_2019.parquet'
    
    print("🔄 Iniciando coleta consolidada (PIB, Pop, Tel, IDHM e Geocode)...")

    query = """
    WITH pib_pop AS (
        SELECT 
            p.id_municipio,
            p.pib,
            pop.populacao
        FROM `basedosdados.br_ibge_pib.municipio` AS p
        INNER JOIN `basedosdados.br_ibge_populacao.municipio` AS pop
            ON p.id_municipio = pop.id_municipio AND p.ano = pop.ano
        WHERE p.ano = 2019
    ),
    telefonia AS (
        SELECT id_municipio, densidade
        FROM `basedosdados.br_anatel_telefonia_movel.densidade_municipio`
        WHERE ano = 2019 AND mes = 12
    ),
    idhm AS (
        SELECT id_municipio, idhm_e
        FROM `basedosdados.mundo_onu_adh.municipio`
        WHERE ano = 2010
    ),
    diretorio AS (
        SELECT 
            id_municipio, 
            nome, 
            sigla_uf,
            ST_X(centroide) AS longitude,
            ST_Y(centroide) AS latitude
        FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    )
    SELECT 
        d.id_municipio,
        d.nome,
        d.sigla_uf,
        d.longitude,
        d.latitude,
        p.populacao,
        t.densidade AS densidade_tel,
        i.idhm_e,
        (p.pib / p.populacao) AS pib_per_capita
    FROM diretorio d
    INNER JOIN pib_pop p ON d.id_municipio = p.id_municipio
    LEFT JOIN telefonia t ON d.id_municipio = t.id_municipio
    LEFT JOIN idhm i ON d.id_municipio = i.id_municipio
    """

    try:
        df = bd.read_sql(query=query, billing_project_id=billing_id)
        
        if not df.empty:
            # Tipagem para performance
            df['populacao'] = df['populacao'].astype(int)
            cols_float = ['longitude', 'latitude', 'densidade_tel', 'idhm_e', 'pib_per_capita']
            df[cols_float] = df[cols_float].astype(float)
            
            # Garantir diretório e salvar
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_parquet(output_path, index=False, compression='snappy')
            
            print(f"✅ Dataset pronto! {len(df)} municípios processados.")
            print(f"📂 Local: {output_path}")
            print(f"📊 Colunas: {df.columns.tolist()}")
        else:
            print("❌ A consulta não retornou dados.")

    except Exception as e:
        print(f"⚠️ Erro: {e}")

if __name__ == "__main__":
    main()