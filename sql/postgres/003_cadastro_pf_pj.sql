-- ===================================================================
-- CADASTRO DE PESSOA FISICA (b-Cadastros CPF)
-- ===================================================================
CREATE TABLE IF NOT EXISTS admb_cads.cad_cpf (
  num_cpf                char(11) PRIMARY KEY,
  nom_pessoa             text NOT NULL,
  nom_social             text,
  nom_mae                text,
  dat_nascimento         date,
  ano_obito              integer,
  dat_inscricao          date,
  dat_ultima_atualiz     date,
  cod_sit_cad_pf         text,
  cod_sexo               text,
  ind_residente_exterior text,
  ind_estrangeiro        text,
  cod_municipio_dom_rfb  text,
  uf_municipio_dom       char(2),
  logradouro             text,
  num_logradouro         text,
  complemento            text,
  bairro                 text,
  cep                    text,
  cod_municipio_nat_rfb  text,
  uf_municipio_nat       char(2),
  cod_pais_nacionalidade text,
  cod_pais_residencia    text,
  des_email              text,
  num_telefone           text,
  data_carga             timestamptz DEFAULT now(),
  CONSTRAINT fk_cpf_mun_dom FOREIGN KEY (cod_municipio_dom_rfb)
    REFERENCES admb_cads.cad_municipio (cod_municipio_rfb),
  CONSTRAINT fk_cpf_mun_nat FOREIGN KEY (cod_municipio_nat_rfb)
    REFERENCES admb_cads.cad_municipio (cod_municipio_rfb),
  CONSTRAINT fk_cpf_pais_nac FOREIGN KEY (cod_pais_nacionalidade)
    REFERENCES admb_cads.cad_pais (cod_pais),
  CONSTRAINT fk_cpf_pais_res FOREIGN KEY (cod_pais_residencia)
    REFERENCES admb_cads.cad_pais (cod_pais)
);

-- ===================================================================
-- CADASTRO DE PESSOA JURIDICA (b-Cadastros CNPJ)
-- ===================================================================
CREATE TABLE IF NOT EXISTS admb_cads.cadastro (
  num_cnpj_raiz      char(8) PRIMARY KEY,
  nome_empresarial   text NOT NULL,
  natureza_juridica  text REFERENCES admb_cads.cad_natureza_juridica (num_natureza_juridica),
  porte_empresa      text,
  capital_social     numeric,
  cpf_responsavel    char(11) REFERENCES admb_cads.cad_cpf (num_cpf),
  qualificacao_resp  text REFERENCES admb_cads.cad_tipo_socio (num_tipo_socio),
  data_inclusao_resp date,
  data_carga         timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS admb_cads.estabelecimento (
  num_cnpj              char(14) PRIMARY KEY,
  num_cnpj_raiz         char(8) REFERENCES admb_cads.cadastro (num_cnpj_raiz),
  nome_fantasia         text,
  indicador_matriz      char(1),
  situacao_cadastral    text,
  data_situcada         date,
  motivo_situcada       text REFERENCES admb_cads.cad_motivo_situcada (cod_motivo_situcada),
  tipo_logradouro       text,
  logradouro            text,
  numero                text,
  complemento           text,
  bairro                text,
  cep                   text,
  cod_municipio_rfb     text REFERENCES admb_cads.cad_municipio (cod_municipio_rfb),
  uf                    char(2),
  email                 text,
  telefone1             text,
  telefone2             text,
  contador_pf           char(11) REFERENCES admb_cads.cad_cpf (num_cpf),
  contador_pj           char(14) REFERENCES admb_cads.estabelecimento (num_cnpj),
  uf_crc_contador_pf    char(2),
  uf_crc_contador_pj    char(2),
  seq_crc_contador_pf   text,
  seq_crc_contador_pj   text,
  tipo_crc_contador_pf  text,
  tipo_crc_contador_pj  text,
  data_inicio_atividade date,
  cnae_principal        text REFERENCES admb_cads.cad_atividades (num_atv),
  data_carga            timestamptz DEFAULT now()
);

-- ===================================================================
-- QSA (Quadro Societario e Administradores) (b-Cadastros CNPJ)
-- ===================================================================
CREATE TABLE IF NOT EXISTS admb_cads.qsa (
  id_qsa             bigserial PRIMARY KEY,
  num_cnpj           char(14) REFERENCES admb_cads.estabelecimento (num_cnpj),
  cpf_socio          char(11) REFERENCES admb_cads.cad_cpf (num_cpf),
  cnpj_socio         char(14) REFERENCES admb_cads.estabelecimento (num_cnpj),
  qualificacao_socio text REFERENCES admb_cads.cad_tipo_socio (num_tipo_socio),
  tipo_socio         char(1) CHECK (tipo_socio IN ('F','J','E')),
  cpf_representante  char(11) REFERENCES admb_cads.cad_cpf (num_cpf),
  qualificacao_rep   text REFERENCES admb_cads.cad_tipo_socio (num_tipo_socio),
  data_entrada       date,
  data_carga         timestamptz DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_qsa_pf
  ON admb_cads.qsa (num_cnpj, cpf_socio)
  WHERE cpf_socio IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_qsa_pj
  ON admb_cads.qsa (num_cnpj, cnpj_socio)
  WHERE cnpj_socio IS NOT NULL;


-- ===================================================================
-- Atividades Secundarias (b-Cadastros CNPJ)
-- ===================================================================
CREATE TABLE IF NOT EXISTS admb_cads.atividades_secundarias (
  id_atv_sec         bigserial PRIMARY KEY,
  num_cnpj           char(14) REFERENCES admb_cads.estabelecimento (num_cnpj),
  cnae_secundaria    text REFERENCES admb_cads.cad_atividades (num_atv),
  data_carga         timestamptz DEFAULT now()
);

-- ===================================================================
-- SIMPLES NACIONAL / MEI (b-Cadastros SN)
-- ===================================================================
CREATE TABLE IF NOT EXISTS admb_cads.cad_simples_nacional (
  num_cnpj_raiz         char(8) PRIMARY KEY,
  ind_simples_ativo     char(1) CHECK (ind_simples_ativo IN ('S','N')),
  ind_mei_ativo         char(1) CHECK (ind_mei_ativo IN ('S','N')),
  dat_opcao_simples     date,
  dat_exclusao_simples  date,
  dat_opcao_mei         date,
  dat_exclusao_mei      date,
  motivo_exclusao_simples text,
  motivo_exclusao_mei     text,
  situacao_simples      text,
  situacao_mei          text,
  data_carga            timestamptz DEFAULT now(),
  FOREIGN KEY (num_cnpj_raiz)
    REFERENCES admb_cads.cadastro (num_cnpj_raiz)
);
