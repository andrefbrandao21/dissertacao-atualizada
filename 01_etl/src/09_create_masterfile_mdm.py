import pandas as pd
import numpy as np
import os

def limpar_id(series):
    """
    Padroniza o ID do município: remove decimais (.0), 
    converte para inteiro e preenche com zeros à esquerda (7 dígitos).
    """
    return pd.to_numeric(series, errors='coerce').fillna(0).astype(int).astype(str).str.zfill(7)

def carregar_e_preparar_dados():
    """Carrega as bases brutas e processadas aplicando a limpeza de IDs."""
    # 1. Base do Pix (Cross-Section)
    df_pix = pd.read_parquet('../data/processed/intensidade_pix_municipios.parquet')
    df_pix['id_municipio'] = limpar_id(df_pix['id_municipio'])

    # 2. Covariáveis (Socioeconômicas e Geo)
    df_covar = pd.read_parquet('../data/raw/covariaveis_municipais.parquet')
    df_covar['id_municipio'] = limpar_id(df_covar['id_municipio'])

    # 3. Homicídios (Ipeadata)
    df_homic = pd.read_csv('../data/processed/homicidios_ipeadata.csv', skiprows=1, sep=',')
    df_homic = df_homic.rename(columns={'2019': 'taxa_homicidio', 'Código': 'cod_raw'})
    df_homic['id_municipio'] = limpar_id(df_homic['cod_raw'])
    
    return df_pix, df_covar, df_homic

def realizar_merges(df_pix, df_covar, df_homic):
    """Executa a união das bases e cria a coluna de macrorregião."""
    # Pix + Covariáveis
    cols_drop = ['nome', 'sigla_uf', 'populacao', 'Municipio', 'Estado']
    df_final = df_pix.merge(
        df_covar.drop(columns=cols_drop, errors='ignore'), 
        on='id_municipio', 
        how='left'
    )

    # Adição dos Homicídios
    df_final = df_final.merge(
        df_homic[['id_municipio', 'taxa_homicidio']], 
        on='id_municipio', 
        how='left'
    )

    # CRIAR COLUNA DE MACRORREGIÃO (Dígito 1 do ID IBGE)
    # 1: Norte, 2: Nordeste, 3: Sudeste, 4: Sul, 5: Centro-Oeste
    df_final['cod_regiao'] = df_final['id_municipio'].str[0]
    
    return df_final

def tratar_e_salvar(df_final, output_path):
    """Trata valores ausentes e salva o arquivo final em parquet."""
    cols_num = [
        'taxa_homicidio', 'pib_per_capita', 'idhm_e', 
        'densidade_tel', 'populacao', 'longitude', 'latitude'
    ]

    for col in cols_num:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_final.to_parquet(output_path, index=False)
    
    print(f"✅ Merge concluído com sucesso!")
    print(f"📊 Coluna 'cod_regiao' criada para Exact Match.")
    print(f"📌 Arquivo salvo em: {output_path}")

def main():
    output_path = '../data/processed/dataset_final_matching.parquet'
    df_pix, df_covar, df_homic = carregar_e_preparar_dados()
    df_final = realizar_merges(df_pix, df_covar, df_homic)
    tratar_e_salvar(df_final, output_path)

if __name__ == "__main__":
    main()