
# Impacto do PIX na Formalização e Estoque de MEIs

Este repositório contém o pipeline completo da minha dissertação de mestrado em Economia (UFPB), analisando o impacto da tecnologia de pagamentos instantâneos sobre a dinâmica do mercado de trabalho formalizado no Brasil.

## 📁 Estrutura do Repositório

### `01_etl/`

Contém todo o processamento de microdados brutos até a geração do painel balanceado.

* **src/**: Scripts Python otimizados para processamento em larga escala (CNPJ/RFB e RAIS).
* **queries/**: Consultas SQL para extração agregada via BigQuery (Base dos Dados).
* **Principais fluxos**:
* `05_transform_cnpj.py`: Tratamento de fluxos de entrada/saída de MEIs.
* `06_balanceamento.py`: Garantia de painel completo para o modelo econométrico.



### `02_modeling/`

Estimações econométricas realizadas em **R**.

* `01_model_twfe.R`: Estimações de Two-Way Fixed Effects e Callaway & Sant'Anna (2021).
* `outputs/`: Resultados gerados (tabelas e gráficos de eventos).

### `03_writing/`

Arquivos fonte da dissertação em **LaTeX**.

* `main.tex`: Arquivo principal do texto.
* `references.bib`: Base de referências bibliográficas.

---

## 🛠️ Tecnologias Utilizadas

* **Python**: ETL e manipulação de dados (Pandas, Parquet).
* **SQL (BigQuery)**: Agregação eficiente de microdados da RAIS.
* **R**: Estimações econométricas (pacotes `did`, `fixest`).
* **LaTeX**: Redação acadêmica.

## 📊 Dados Utilizados

* **CNPJ (Receita Federal)**: Dados de estabelecimentos e opção pelo MEI.
* **RAIS**: Estoque de vínculos ativos para controle de heterogeneidade.
* **PIX (BCB)**: Transações municipais via API de Dados Abertos.

---

## 📝 Como usar

1. Execute os scripts de `01_etl/src/` em ordem numérica para gerar o arquivo final no diretório `data/processed/` (não versionado).
2. Utilize o diretório `02_modeling/` para replicar as estimações.

