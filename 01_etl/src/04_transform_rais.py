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
    Garante que cada município tenha os 4 setores em todos os 9 anos.
    """
    print("⚖️  [1/2] Criando grid completo (Município x Setor x Ano)...")
    
    # 1. Definição dos eixos
    todos_muns = df['id_municipio'].unique()
    todos_sets = ['Agro', 'Industria', 'Servicos', 'Setor Publico']
    todos_anos = sorted(df['ano'].unique())
    
    # 2. Criar o MultiIndex para o Produto Cartesiano
    index_completo = pd.MultiIndex.from_product(
        [todos_muns, todos_sets, todos_anos], 
        names=['id_municipio', 'setor', 'ano']
    )
    
    # 3. Reindexar (Preenche lacunas com zero)
    print("⚙️  [2/2] Reindexando e calculando log_estoque...")
    # Removendo nome_municipio temporariamente para evitar duplicidade no reindex
    df_balanceado = (df.set_index(['id_municipio', 'setor', 'ano'])
                     .reindex(index_completo, fill_value=0)
                     .reset_index())
    
    # 4. Recuperar nomes dos municípios
    nomes = df[['id_municipio', 'nome_municipio']].drop_duplicates()
    df_balanceado = df_balanceado.drop(columns=['nome_municipio'], errors='ignore').merge(nomes, on='id_municipio', how='left')
    
    # 5. Variável Econométrica
    df_balanceado['log_estoque'] = np.log1p(df_balanceado['quantidade_vinculos_ativos'].astype(np.float32))
    
    return df_balanceado

def main():
    print("--- INICIANDO TRANSFORMAÇÃO RAIS ---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Erro: Arquivo {INPUT_FILE} não encontrado.")
        return

    # Leitura e Processamento
    df_raw = pd.read_parquet(INPUT_FILE)
    
    # Filtro preventivo (caso existam nulos ou setores 'Outros')
    df_raw = df_raw.dropna(subset=['id_municipio'])
    df_raw = df_raw[df_raw['setor'] != 'Outros']
    
    df_final = balancear_painel_final(df_raw)
    
    # Salvamento
    print(f"💾 Salvando painel balanceado ({len(df_final):,} linhas)...")
    df_final.to_parquet(OUTPUT_FILE, index=False, compression='snappy')
    
    del df_raw, df_final
    gc.collect()
    print("--- CONCLUÍDO COM SUCESSO ---")

if __name__ == "__main__":
    main()