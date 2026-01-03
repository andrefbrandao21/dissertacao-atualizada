import pandas as pd
import numpy as np
import os

# --- CONFIGURAÇÃO ---
INPUT_FILE = '../data/processed/painel_mei_rf_anual.parquet'
OUTPUT_FILE = '../data/processed/painel_mei_balanceado_2016_2024.parquet'

def balancear_dados(df):
    print("⚖️ Iniciando balanceamento do painel (2016-2024)...")
    
    # 1. Definição dos eixos do grid [cite: 17, 36]
    # Filtramos apenas os municípios presentes na base
    todos_muns = df['mun'].unique()
    
    # Setores definidos conforme sua regra de negócio [cite: 35, 36]
    todos_setores = ['Agro', 'Industria', 'Servicos', 'Setor Publico']
    
    # Período solicitado
    todos_anos = np.arange(2016, 2025)
    
    # 2. Criar o MultiIndex (Produto Cartesiano)
    # Isso gera todas as combinações possíveis de Mun x Setor x Ano
    index_completo = pd.MultiIndex.from_product(
        [todos_muns, todos_setores, todos_anos],
        names=['mun', 'setor', 'ano']
    )
    
    # 3. Reindexar preenchendo lacunas com 0
    # O reindex é mais rápido que o merge para criar grids vazios
    df_balanceado = (df.set_index(['mun', 'setor', 'ano'])
                     .reindex(index_completo, fill_value=0)
                     .reset_index())
    
    # 4. Recalcular Estoque Acumulado após o balanceamento
    # Essencial para garantir que anos "vazios" mantenham o estoque do ano anterior
    print("📊 Recalculando estoques e logaritmos...")
    df_balanceado = df_balanceado.sort_values(['mun', 'setor', 'ano'])
    
    # O estoque acumulado deve considerar as entradas e saídas agora balanceadas
    df_balanceado['estoque_mei'] = (
        df_balanceado.groupby(['mun', 'setor'])['entradas'].cumsum() - 
        df_balanceado.groupby(['mun', 'setor'])['saidas'].cumsum()
    )
    
    # Variável para o modelo Callaway & Sant'Anna (log de x + 1)
    df_balanceado['log_estoque'] = np.log1p(df_balanceado['estoque_mei'].clip(lower=0))
    
    return df_balanceado

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Erro: Arquivo {INPUT_FILE} não encontrado.")
        return

    # Carregando dados
    df = pd.read_parquet(INPUT_FILE)
    
    # Executando balanceamento
    df_final = balancear_dados(df)
    
    # Salvando
    df_final.to_parquet(OUTPUT_FILE, index=False)
    print(f"✅ Painel balanceado salvo com {len(df_final):,} linhas.")
    print(f"📂 Caminho: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()