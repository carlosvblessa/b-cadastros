-- Tabelas de cadastros (assume schema admb_cads ja criado)

-- CNAE
CREATE TABLE IF NOT EXISTS admb_cads.cad_atividades (
  num_atv   text PRIMARY KEY,
  desc_atv  text NOT NULL
);

-- Natureza juridica
CREATE TABLE IF NOT EXISTS admb_cads.cad_natureza_juridica (
  num_natureza_juridica text PRIMARY KEY,
  dsc_natureza_juridica text NOT NULL
);

-- Qualificacao de socio / responsavel
CREATE TABLE IF NOT EXISTS admb_cads.cad_tipo_socio (
  num_tipo_socio text PRIMARY KEY,
  desc_tipo_socio text NOT NULL
);

-- Motivo da situacao cadastral
CREATE TABLE IF NOT EXISTS admb_cads.cad_motivo_situcada (
  cod_motivo_situcada text PRIMARY KEY,
  dsc_motivo_situcada text NOT NULL
);

-- Paises
CREATE TABLE IF NOT EXISTS admb_cads.cad_pais (
  cod_pais   text PRIMARY KEY,
  nom_pais   text NOT NULL
);

-- Municipios (codigo RFB, nome, UF)
CREATE TABLE IF NOT EXISTS admb_cads.cad_municipio (
  cod_municipio_rfb text PRIMARY KEY,
  nom_municipio     text NOT NULL
);
