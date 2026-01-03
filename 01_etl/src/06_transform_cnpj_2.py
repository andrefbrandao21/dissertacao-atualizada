import pandas as pd
import numpy as np
import os

# --- CONFIGURAÇÃO ---
INPUT_FILE = '../data/processed/painel_mei_rf_anual.parquet'
OUTPUT_FILE = '../data/processed/painel_mei_balanceado_2016_2024.parquet'

def balancear_dados(df):
    print("📊 [1/4] Criando setor Total (Soma dos fluxos)...")
    
    # 1. Gerar o Total a partir dos fluxos de entrada/saída
    # Agrupamos por município e ano, ignorando o setor original
    df_total = df.groupby(['mun', 'ano'], as_index=False)[['entradas', 'saidas']].sum()
    df_total['setor'] = 'Total'
    
    # Empilha o Total com os setores originais
    df = pd.concat([df, df_total], ignore_index=True)

    print("⚖️ [2/4] Iniciando balanceamento do painel (2016-2024)...")
    
    todos_muns = df['mun'].unique()
    # Adicionado 'Total' na lista explicitamente
    todos_setores = ['Agro', 'Industria', 'Servicos', 'Setor Publico', 'Total']
    todos_anos = np.arange(2016, 2025)
    
    # 2. Criar o MultiIndex (Produto Cartesiano)
    index_completo = pd.MultiIndex.from_product(
        [todos_muns, todos_setores, todos_anos],
        names=['mun', 'setor', 'ano']
    )
    
    # 3. Reindexar preenchendo lacunas com 0
    df_balanceado = (df.set_index(['mun', 'setor', 'ano'])
                     .reindex(index_completo, fill_value=0)
                     .reset_index())
    
    # 4. Recalcular Estoque Acumulado
    print("📈 [3/4] Recalculando estoques acumulados por setor e total...")
    df_balanceado = df_balanceado.sort_values(['mun', 'setor', 'ano'])
    
    # O estoque acumulado agora funciona perfeitamente para setores e para o total
    df_balanceado['estoque_mei'] = (
        df_balanceado.groupby(['mun', 'setor'])['entradas'].cumsum() - 
        df_balanceado.groupby(['mun', 'setor'])['saidas'].cumsum()
    )
    
    # 5. Logaritmo para DiD
    print("🧪 [4/4] Aplicando log_estoque...")
    df_balanceado['log_estoque'] = np.log1p(df_balanceado['estoque_mei'].clip(lower=0))
    
    return df_balanceado

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Erro: Arquivo {INPUT_FILE} não encontrado.")
        return

    # Carregando dados (garantindo que mun seja int para o csdid)
    df = pd.read_parquet(INPUT_FILE)
    df['mun'] = df['mun'].astype(int)
    
    # Executando balanceamento
    df_final = balancear_dados(df)
    
    # Salvando
    df_final.to_parquet(OUTPUT_FILE, index=False)
    print(f"✅ Painel balanceado e totalizado salvo com {len(df_final):,} linhas.")
    print(f"📂 Caminho: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()