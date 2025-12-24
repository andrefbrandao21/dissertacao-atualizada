import pandas as pd
import numpy as np
from tqdm import tqdm
import gc
import os
from pathlib import Path

# --- CONFIGURAÇÃO DE CAMINHOS ---
BASE_PATH = "../data/raw/CNPJ"
# Lista os 10 arquivos de estabelecimentos conforme o layout da RFB [cite: 3, 16]
ARQUIVOS_ESTAB = [os.path.join(BASE_PATH, f"Estabelecimentos{i}/K3241.K03200Y{i}.D50913.ESTABELE") for i in range(10)]
CAMINHO_SIMPLES = os.path.join(BASE_PATH, "Simples/F.K03200$W.SIMPLES.CSV.D50913")
OUTPUT_DIR = "../data/processed"
SAIDA_FINAL = os.path.join(OUTPUT_DIR, 'painel_mei_rf_anual.parquet')

def main():
    print("--- INICIANDO ETL MEI OTIMIZADO E CORRIGIDO ---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Carregar Simples: Contém os indicadores de opção pelo MEI e as datas [cite: 20]
    print("🚀 Carregando base do Simples...")
    df_simples = pd.read_csv(
        CAMINHO_SIMPLES, 
        sep=";", 
        encoding="latin-1", 
        header=None, 
        usecols=[0, 4, 5, 6], 
        names=["cnpj_basico", "opcao_mei", "data_ini", "data_fim"],
        dtype={"cnpj_basico": "int32", "opcao_mei": "str", "data_ini": "str", "data_fim": "str"}
    )
    
    # Filtra apenas quem é ou já foi optante pelo MEI [cite: 20]
    df_simples = df_simples[df_simples["opcao_mei"] == "S"].copy()
    
    # Converte datas para Ano via vetorização (YYYYMMDD -> YYYY) 
    df_simples['ano_ini'] = pd.to_numeric(df_simples['data_ini'], errors='coerce').fillna(0) // 10000
    df_simples['ano_fim'] = pd.to_numeric(df_simples['data_fim'], errors='coerce').fillna(0) // 10000
    
    # Limpeza: Mantemos apenas anos razoáveis para evitar o "ano zero"
    df_simples = df_simples.drop(columns=["opcao_mei", "data_ini", "data_fim"])
    gc.collect()

    painel_final_lista = []

    # 2. Loop Estabelecimentos: Contém CNAE e Código de Município [cite: 3, 17]
    for path in ARQUIVOS_ESTAB:
        if not os.path.exists(path):
            continue
        
        painel_arquivo = []
        # Chunksize grande para velocidade, mas monitorando a RAM [cite: 38, 39]
        reader = pd.read_csv(
            path, 
            sep=";", 
            encoding="latin-1", 
            header=None,
            usecols=[0, 11, 20], 
            names=["cnpj_basico", "cnae", "mun"],
            dtype={"cnpj_basico": "int32", "cnae": "str", "mun": "float64"},
            chunksize=2_000_000
        )
        
        for chunk in tqdm(reader, desc=f"📂 {os.path.basename(path)}"):
            # Cruzamento interno: só processamos quem está na lista de MEIs [cite: 5, 11]
            chunk = chunk.merge(df_simples, on="cnpj_basico", how="inner")
            
            # Mapeamento Setorial (Divisão CNAE - 2 primeiros dígitos) [cite: 17, 36]
            div = chunk['cnae'].str[:2].fillna('00')
            chunk['setor'] = 'Servicos'
            chunk.loc[div.isin(['01', '02', '03']), 'setor'] = 'Agro'
            chunk.loc[div.astype(int).between(5, 33), 'setor'] = 'Industria'
            chunk.loc[div == '84', 'setor'] = 'Setor Publico'
            
            # Agregação de Entradas: Apenas anos válidos (> 1900)
            ent = chunk[chunk['ano_ini'] > 1900].groupby(['mun', 'setor', 'ano_ini']).size().reset_index(name='entradas')
            ent.columns = ['mun', 'setor', 'ano', 'entradas']
            
            # Agregação de Saídas: CRUCIAL - Ignora NaTs/Zeros para evitar estoque negativo
            sai = chunk[chunk['ano_fim'] > 1900].groupby(['mun', 'setor', 'ano_fim']).size().reset_index(name='saidas')
            sai.columns = ['mun', 'setor', 'ano', 'saidas']
            
            # Merge de Fluxo
            fluxo = pd.merge(ent, sai, on=['mun', 'setor', 'ano'], how='outer').fillna(0)
            painel_arquivo.append(fluxo)
            
            del chunk, ent, sai, fluxo
            gc.collect()
            
        if painel_arquivo:
            # Consolida o arquivo atual antes de ir para o próximo (previne fragmentação)
            df_arq = pd.concat(painel_arquivo).groupby(['mun', 'setor', 'ano']).sum().reset_index()
            painel_final_lista.append(df_arq)
            del painel_arquivo
            gc.collect()

    # 3. Consolidação e Cálculo de Estoque
    print("\n📊 Gerando painel final e calculando estoques...")
    df_final = pd.concat(painel_final_lista).groupby(['mun', 'setor', 'ano']).sum().reset_index()
    
    # Ordenação necessária para o cálculo acumulado (Time-series)
    df_final = df_final.sort_values(['mun', 'setor', 'ano'])
    
    # Estoque = Soma acumulada de quem entrou (-) soma acumulada de quem saiu
    df_final['estoque_mei'] = (
        df_final.groupby(['mun', 'setor'])['entradas'].cumsum() - 
        df_final.groupby(['mun', 'setor'])['saidas'].cumsum()
    )

    # Exportação em Parquet para performance no modelo econométrico
    df_final.to_parquet(SAIDA_FINAL, index=False)
    print(f"✅ Painel concluído com sucesso: {SAIDA_FINAL}")

if __name__ == "__main__":
    main()