# b-cadastros

Projeto para cadastros iniciais usando R, PostgreSQL e CouchDB.

## Estrutura
- `scripts/setup_postgres.R`: cria/valida o schema e aplica todos os SQLs em `sql/postgres`.
- `scripts/load_dominios_cnpj.R`: rotina de carga dos dominios CNPJ (mensal) a partir dos ZIPs da RFB, com controle de pastas e status.
- `scripts/load_from_couch_cnpjs.R`: rotina de carga seletiva (CPF/CNPJ/SN) a partir dos bancos CouchDB para uma lista de CNPJs, com BFS para trazer socios PJ/contadores PJ referenciados e commits em lotes. Ignora e loga QSA sem chave (CPF/CNPJ vazio) e faz upsert com `ON CONFLICT` alinhado aos índices únicos para não duplicar.
- `sql/postgres/001_create_cadastros.sql`: define as tabelas de cadastro no schema `admb_cads` (o script cria o schema se houver permissao).
- `sql/postgres/002_ctrl_carga_dominios.sql`: cria tabela de controle `admb_cads.ctrl_carga_dominios_cnpj`.
- `sql/postgres/003_cadastro_pf_pj.sql`: cria estruturas de CPF, CNPJ (cadastro e estabelecimentos), QSA, Simples Nacional e atividades secundarias (FK de contador PJ já incluída). Inclui índices únicos em QSA para evitar duplicidade de sócios PF/PJ por CNPJ.
- `scripts/reprocess_simples_mei.R`: reprocessa Simples/MEI para corrigir inconsistencias de situacao (usa CouchDB `chsn_bcadastros_replica`).
- `couchdb/`: espaco para configuracao de bancos e design docs do CouchDB.
- `R/`: funcoes auxiliares que venham a ser compartilhadas entre os scripts R.

## Como rodar a estrutura no PostgreSQL
1. Garanta o `.env` com as variaveis `DB_NAME`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`.
2. Instale os pacotes R necessarios (uma vez):\
   `install.packages(c("dotenv", "DBI", "RPostgres", "readr"))`
3. Garanta que o usuario do `.env` tenha `CREATE` no database e `USAGE, CREATE` no schema `admb_cads`. Sem isso, peca ao DBA para rodar algo como:\
   `CREATE SCHEMA IF NOT EXISTS admb_cads AUTHORIZATION user_cadapi;`\
   `GRANT USAGE, CREATE ON SCHEMA admb_cads TO user_cadapi;`
4. Execute o script de setup (estrutura/tabelas):\
   `Rscript scripts/setup_postgres.R`

## Carga rotineira dos dominios CNPJ
1. Instale pacotes (alem dos anteriores):\
   `install.packages(c("httr", "dplyr", "glue"))`
2. Opcional:\
   - Forcar uma pasta especifica de CNPJ: `CNPJ_PASTA_REF=2025-11`\
   - Forcar recarga mesmo que ja exista OK: `CNPJ_FORCE_RELOAD=true`
3. Rode a carga periodica:\
   `Rscript scripts/load_dominios_cnpj.R`\
   - O script detecta a pasta, baixa cada ZIP, trunca e recarrega as tabelas, registra status em `admb_cads.ctrl_carga_dominios_cnpj` e nao recarrega o que ja esta OK para a mesma pasta **a menos que** o header HTTP `Last-Modified` do ZIP seja mais novo que a `data_carga` registrada ou `CNPJ_FORCE_RELOAD` esteja ativo.

## Carga seletiva a partir do CouchDB (CPF/CNPJ/SN)
1. Pacotes: `install.packages(c("httr", "jsonlite", "stringr"))` (além dos basicos de DBI/RPostgres/dotenv).
2. Exige variaveis de Couch no `.env`: `COUCHDB_SCHEME`, `COUCHDB_HOST`, `COUCHDB_PORT`, `COUCHDB_USER`, `COUCHDB_PASSWORD` (aliases `COUCH_*` tambem funcionam). Bancos default: `chcnpj_bcadastros_replica`, `chcpf_bcadastros_replica`, `chsn_bcadastros_replica`.
3. Lista de CNPJs: por default `select cnpj from admcadapi.lista2_cnpjs`; altere via `CNPJ_LIST_QUERY` se precisar.
4. Rode: `Rscript scripts/load_from_couch_cnpjs.R`\
   - Para cada CNPJ, pega raiz (8) para cadastro e Simples, CNPJ completo para estabelecimento, grava o CPF do responsavel (se nao existir), carrega socios (distinguindo F/J/E via campo `tipo`), popula QSA (agora com `cpf_socio`/`cnpj_socio` separados) e atividades secundarias em tabela própria. Executa em BFS enfileirando socios PJ e contadores PJ referenciados. Usa consultas ao Couch em lote (`COUCH_BATCH_SIZE`, default 200) e commits por lote (`BATCH_COMMIT_SIZE`, default 50); pendencias de contador PJ e QSA são resolvidas a cada commit e no final.

## Reprocessar Simples/MEI
- Corrige linhas de `admb_cads.cad_simples_nacional` onde o indicador esta `S` mas ha data de exclusao.\
  `Rscript scripts/reprocess_simples_mei.R`\
  - Usa CouchDB `chsn_bcadastros_replica` para recalcular status atual (ultimo periodo) de Simples/MEI por raiz de CNPJ.

## Proximos passos para CouchDB
- Guarde design docs e arquivos auxiliares em `couchdb/design_docs`.
- Para criar um banco via HTTP basico:\
  `curl -X PUT http://COUCHDB_USER:COUCHDB_PASSWORD@COUCHDB_HOST:COUCHDB_PORT/nome_do_banco`
- Para carregar um design doc:\
  `curl -X PUT http://user:pass@host:port/nome_do_banco/_design/minha_view -H "Content-Type: application/json" -d @couchdb/design_docs/minha_view.json`

> Observacao: o caractere `#` funciona como comentario em `.env`; caso a senha contenha `#`, use aspas ou escape para que o valor completo seja carregado.
