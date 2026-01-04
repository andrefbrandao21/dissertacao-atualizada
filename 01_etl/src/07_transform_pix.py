import pandas as pd

# 1. Carregar a base
df = pd.read_parquet('../data/raw/dados_pix.parquet')

# 2. Ordenar cronologicamente para garantir que os "6 primeiros" sejam os corretos
df = df.sort_values(by=['Municipio_Ibge', 'AnoMes'])

# 3. Calcular a intensidade por município
# Usamos groupby + transform para manter o alinhamento e depois reduzir a base
df['intensidade_pix_bruta'] = (
    df.groupby('Municipio_Ibge')['QT_PagadorPF']
    .transform(lambda x: x.iloc[:6].mean())
)

# 4. Criar o Cross-Section
# Selecionamos apenas as colunas de identificação e a nova variável
# O drop_duplicates garante que teremos apenas uma linha por município
df_cross_section = df[['Municipio_Ibge', 'Municipio', 'Estado', 'intensidade_pix_bruta']].drop_duplicates()

# 5. Resetar o index para ficar limpo
df_cross_section = df_cross_section.reset_index(drop=True)

# 6. Salvar ou Visualizar
print(f"Total de municípios processados: {len(df_cross_section)}")
print(df_cross_section.head())

# Para salvar o resultado:
df_cross_section.to_csv('../data/processed/intensidade_pix_municipios.csv', index=False)