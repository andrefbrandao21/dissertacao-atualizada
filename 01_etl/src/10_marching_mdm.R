# ===============================================
# Pareamento Mahalanobis + Spatial Buffer no R
# ===============================================
library(dplyr)
library(MatchIt)
library(arrow)   # Para ler o arquivo .parquet do Python
library(fields)  # Para rdist.earth

# 1. Carregar os dados limpos do Python
df <- read_parquet('G:\\Meu Drive\\impacto-pix-mei-ajustado\\dissertacao-pix\\01_etl\\data\\processed\\dataset_final_matching.parquet')

# 2. Spatial Buffer: Remover controles próximos antes do matching
coords <- as.matrix(df[, c("longitude", "latitude")])
geo_mat <- rdist.earth(coords, miles = FALSE) # Distância em km
BUFFER_KM <- 30

# Índices para cada grupo (ajuste para o tratamento que quiser testar)
treated_idx <- which(df$treat_quintil == 1)
control_idx <- which(df$treat_quintil == 0)

# Identificar controles a menos de 30km de QUALQUER tratado
dist_tc <- geo_mat[treated_idx, control_idx, drop = FALSE]
close_control_idx <- control_idx[apply(dist_tc < BUFFER_KM, 2, any)]

# Filtrar base para o matching
df_match_input <- df[-close_control_idx, ] %>% filter(!is.na(treat_quintil))

# 3. Executar o Matching de Mahalanobis
match_mdm <- matchit(
  treat_quintil ~ idhm_e + densidade_tel + log_populacao + log_pib_per_capita + taxa_homicidio,
  data = df_match_input,
  method = "nearest",
  distance = "mahalanobis",
  exact = ~ cod_regiao,  # Exact match pela coluna que criamos no Python
  replace = FALSE,
  caliper = 0.25         # Ajuste o caliper conforme a necessidade de balanço
)

# 4. Criar a coluna de identificação final (0 ou 1)
# Extrair IDs que foram mantidos
df_matched_data <- match.data(match_mdm)
ids_keep <- df_matched_data$id_municipio

# No seu dataframe original (df), criamos a coluna binária
df <- df %>%
  mutate(keep_match_quintil = ifelse(id_municipio %in% ids_keep, 1, 0))

# 5. Salvar para voltar ao Python ou seguir no R
write_parquet(df, "../data/processed/dataset_final_com_flags.parquet")