import pandas as pd
import numpy as np
import os

def calcular_intensidade_bruta(path_pix):
    df = pd.read_parquet(path_pix)
    # Garante ordem cronológica para pegar os 6 meses iniciais corretamente
    df = df.sort_values(by=['Municipio_Ibge', 'AnoMes'])
    
    # Média dos 6 primeiros meses por município
    df['intensidade_pix_bruta'] = (
        df.groupby('Municipio_Ibge')['QT_PagadorPF']
        .transform(lambda x: x.iloc[:6].mean())
    )
    
    # Criar Cross-Section e limpar ID
    df_cs = df[['Municipio_Ibge', 'Municipio', 'Estado', 'intensidade_pix_bruta']].drop_duplicates().copy()
    df_cs['id_municipio'] = pd.to_numeric(df_cs['Municipio_Ibge'], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(7)
    
    return df_cs

def carregar_populacao_2020(path_pop):
    df_pop = pd.read_parquet(path_pop)
    # Filtro de ano e limpeza de tipos
    df_pop = df_pop[pd.to_numeric(df_pop['ano'], errors='coerce') == 2020].copy()
    df_pop['id_municipio'] = pd.to_numeric(df_pop['id_municipio'], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(7)
    df_pop['populacao'] = pd.to_numeric(df_pop['populacao'], errors='coerce')
    
    return df_pop[['id_municipio', 'populacao']]

def atribuir_grupos_tratamento(df, coluna, n_groups):
    # rank(method='first') desempata valores iguais (como o 1.0) para garantir grupos de tamanhos iguais
    ranks = df[coluna].rank(method='first')
    bins = pd.qcut(ranks, n_groups, labels=False) + 1
    
    # 0 = Controle (1º Quantil), 1 = Tratamento (Último Quantil), NaN = Meio
    return np.select([bins == 1, bins == n_groups], [0, 1], default=np.nan)

def main():
    # Caminhos
    raw_pix = '../data/raw/dados_pix.parquet'
    raw_pop = '../data/raw/populacao_agregada.parquet'
    processed_path = '../data/processed/intensidade_pix_municipios.parquet'

    print("🚀 Iniciando processamento...")

    # 1. Processar Intensidade e População
    df_pix = calcular_intensidade_bruta(raw_pix)
    df_pop = carregar_populacao_2020(raw_pop)

    # 2. Merge e Normalização
    df_final = df_pix.merge(df_pop, on='id_municipio', how='inner')
    df_final['intensidade_relativa'] = (df_final['intensidade_pix_bruta'] / df_final['populacao']).clip(upper=1)

    # 3. Criar Quantis de Tratamento
    print("📊 Gerando grupos de tratamento (Tercis, Quartis e Quintis)...")
    df_final['treat_tercil'] = atribuir_grupos_tratamento(df_final, 'intensidade_relativa', 3)
    df_final['treat_quartil'] = atribuir_grupos_tratamento(df_final, 'intensidade_relativa', 4)
    df_final['treat_quintil'] = atribuir_grupos_tratamento(df_final, 'intensidade_relativa', 5)

    # 4. Salvar
    os.makedirs(os.path.dirname(processed_path), exist_ok=True)
    df_final.to_parquet(processed_path, index=False)

    print(f"✅ Processo concluído!\n📂 Salvo em: {processed_path}")
    print(f"📈 Municípios processados: {len(df_final)}")
    print(f"🔝 Máximo Intensidade: {df_final['intensidade_relativa'].max()}")

if __name__ == "__main__":
    main()