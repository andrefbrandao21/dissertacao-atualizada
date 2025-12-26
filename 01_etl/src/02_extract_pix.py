import requests
import pandas as pd
from pathlib import Path

def fetch_pix_data(data_base: str):
    """Obtém dados da API do BCB para um mês específico (YYYYMM)."""
    url = f"https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/TransacoesPixPorMunicipio(DataBase=@DataBase)?@DataBase='{data_base}'&$format=json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get('value', [])
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição: {e}")
        return []

def save_to_parquet(data, output_path):
    """Converte para DataFrame e salva em Parquet."""
    if not data:
        return
    df = pd.DataFrame(data)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"Salvo: {out} ({len(df)} registros)")

if __name__ == "__main__":
    # Exemplo de uso
    mes = '202011'
    raw_data = fetch_pix_data(mes)
    save_to_parquet(raw_data, '../data/raw/dados_pix.parquet')