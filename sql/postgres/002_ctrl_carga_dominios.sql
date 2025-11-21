-- Tabela de controle de cargas de dominios do CNPJ
CREATE TABLE IF NOT EXISTS admb_cads.ctrl_carga_dominios_cnpj (
  id_carga    bigserial PRIMARY KEY,
  pasta_ref   text NOT NULL,     -- ex: '2025-11'
  arquivo     text NOT NULL,     -- ex: 'Naturezas.zip'
  data_carga  timestamptz NOT NULL DEFAULT now(),
  status      text NOT NULL,     -- 'OK' ou 'ERRO'
  msg_erro    text,
  CONSTRAINT uq_ctrl_carga_dominios UNIQUE (pasta_ref, arquivo)
);
