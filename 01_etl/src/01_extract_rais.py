import basedosdados as bd
import pandas as pd
import os
from dotenv import load_dotenv

def main():
    load_dotenv()
    billing_id = os.getenv("BILLING_ID")
    output_path = '../data/raw/rais_agregada_municipio_setor.parquet'
    
    # Anos de interesse
    anos = range(2016, 2025) 
    dfs = []

    for ano in anos:
        print(f"🔄 Baixando e agregando RAIS {ano} por Setor...")
        
        # SQL com a lógica de setorização embutida
        query = f"""
        WITH rais_setorizada AS (
            SELECT
                t1.ano,
                t1.id_municipio,
                t2.nome AS nome_municipio,
                CAST(SUBSTR(t1.cnae_2, 1, 2) AS INT64) AS div,
                t1.quantidade_vinculos_ativos
            FROM `basedosdados.br_me_rais.microdados_estabelecimentos` AS t1
            LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` AS t2
                ON t1.id_municipio = t2.id_municipio
            WHERE t1.ano = {ano}
        )
        SELECT
            ano,
            id_municipio,
            nome_municipio,
            CASE 
                WHEN div BETWEEN 1 AND 3 THEN 'Agro'
                WHEN div BETWEEN 5 AND 33 THEN 'Industria'
                WHEN div = 84 THEN 'Setor Publico'
                WHEN div BETWEEN 35 AND 99 THEN 'Servicos'
                ELSE 'Outros'
            END AS setor,
            SUM(quantidade_vinculos_ativos) AS quantidade_vinculos_ativos
        FROM rais_setorizada
        GROUP BY 1, 2, 3, 4
        """
        
        try:
            df_temp = bd.read_sql(query=query, billing_project_id=billing_id)
            if not df_temp.empty:
                # Removemos 'Outros' aqui para manter o painel limpo para o DID
                df_temp = df_temp[df_temp['setor'] != 'Outros']
                dfs.append(df_temp)
                print(f"✅ {ano} processado.")
        except Exception as e:
            print(f"⚠️ Erro em {ano}: {e}")

    # Empilhamento final
    if dfs:
        df_completo = pd.concat(dfs, ignore_index=True)
        
        # Tipagem para garantir economia de memória no Parquet
        df_completo['ano'] = df_completo['ano'].astype(int)
        df_completo['quantidade_vinculos_ativos'] = df_completo['quantidade_vinculos_ativos'].fillna(0).astype(float)
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_completo.to_parquet(output_path, index=False, compression='snappy')
        print(f"\n📊 Sucesso! Total extraído: {len(df_completo):,} linhas.")
        print(f"📂 Salvo em: {output_path}")
    else:
        print("❌ Nenhum dado foi baixado.")

if __name__ == "__main__":
    main()