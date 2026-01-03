import pandas as pd
import numpy as np
import os
import gc

# --- CONFIGURAÇÃO DE CAMINHOS ---
INPUT_FILE = '../data/raw/rais_agregada_municipio_setor.parquet'
OUTPUT_DIR = '../data/processed'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'rais_painel_balanceado.parquet')

def balancear_painel_final(df):
    """
    Garante que cada município tenha os setores + Total em todos os anos.
    """
    print("⚖️  [1/3] Calculando agregados do setor 'Total'...")
    
    # 1. Gerar o Total por Município/Ano
    # Mantemos colunas extras de ID caso existam (ex: ano_tratamento)
    cols_agregacao = ['id_municipio', 'ano']
    if 'ano_tratamento' in df.columns:
        cols_agregacao.append('ano_tratamento')

    df_total = df.groupby(cols_agregacao, as_index=False)['quantidade_vinculos_ativos'].sum()
    df_total['setor'] = 'Total'

    # Unir com os setores originais
    df = pd.concat([df, df_total], ignore_index=True)

    print("⚙️  [2/3] Criando grid completo (Município x Setor x Ano)...")
    
    todos_muns = df['id_municipio'].unique()
    todos_sets = ['Agro', 'Industria', 'Servicos', 'Setor Publico', 'Total']
    todos_anos = sorted(df['ano'].unique())
    
    index_completo = pd.MultiIndex.from_product(
        [todos_muns, todos_sets, todos_anos], 
        names=['id_municipio', 'setor', 'ano']
    )
    
    # 3. Reindexar (Preenche lacunas com zero)
    df_balanceado = (df.set_index(['id_municipio', 'setor', 'ano'])
                     .reindex(index_completo, fill_value=0)
                     .reset_index())
    
    # 4. Recuperar nomes dos municípios
    nomes = df[['id_municipio', 'nome_municipio']].dropna().drop_duplicates()
    df_balanceado = df_balanceado.drop(columns=['nome_municipio'], errors='ignore').merge(nomes, on='id_municipio', how='left')
    
    print("🧪 [3/3] Calculando logs (Econometria)...")
    # Log calculado sobre a soma (correto para o Total)
    df_balanceado['log_estoque'] = np.log1p(df_balanceado['quantidade_vinculos_ativos'].astype(np.float32))
    
    return df_balanceado

def main():
    print("--- INICIANDO TRANSFORMAÇÃO RAIS ---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Erro: Arquivo {INPUT_FILE} não encontrado.")
        return

    df_raw = pd.read_parquet(INPUT_FILE)
    
    # Filtro preventivo
    df_raw = df_raw.dropna(subset=['id_municipio'])
    df_raw = df_raw[~df_raw['setor'].isin(['Outros', 'Total'])] # Evita duplicar se o Total já existir
    
    df_final = balancear_painel_final(df_raw)
    
    print(f"💾 Salvando painel balanceado ({len(df_final):,} linhas)...")
    df_final.to_parquet(OUTPUT_FILE, index=False, compression='snappy')
    
    del df_raw, df_final
    gc.collect()
    print("--- CONCLUÍDO COM SUCESSO ---")

if __name__ == "__main__":
    main()