SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    dados.quantidade_vinculos_ativos as quantidade_vinculos_ativos,
    dados.cnae_2 as cnae_2
FROM `basedosdados.br_me_rais.microdados_estabelecimentos` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
WHERE dados.ano > 2015
