import basedosdados as bd
import pandas as pd
import os
from dotenv import load_dotenv

def main():
    load_dotenv()
    billing_id = os.getenv("BILLING_ID")
    query_path = '../queries/populacao.sql'
    output_path = '../data/raw/populacao_agregada.parquet'

    # Carregar a query do arquivo .sql
    if not os.path.exists(query_path):
        print(f"❌ Arquivo de query não encontrado em: {query_path}")
        return

    with open(query_path, 'r', encoding='utf-8') as f:
        query = f.read()

    print("🔄 Executando query de população na Base dos Dados...")

    try:
        # Download direto da base agregada (sem loop, se a query já tratar os anos)
        df = bd.read_sql(query=query, billing_project_id=billing_id)
        
        if not df.empty:
            # Garantir diretório e salvar
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_parquet(output_path, index=False, compression='snappy')
            print(f"✅ Sucesso! {len(df):,} linhas salvas em: {output_path}")
        else:
            print("⚠️ Query retornou um DataFrame vazio.")

    except Exception as e:
        print(f"⚠️ Erro na execução: {e}")

if __name__ == "__main__":
    main()