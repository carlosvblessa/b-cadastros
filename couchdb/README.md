# CouchDB

- Guarde design docs em `couchdb/design_docs/` (por exemplo, views e indexes em JSON).
- Conexao usa as variaveis `COUCHDB_SCHEME`, `COUCHDB_HOST`, `COUCHDB_PORT`, `COUCHDB_USER`, `COUCHDB_PASSWORD` definidas no `.env`.
- Criar um banco simples via HTTP:\
  `curl -X PUT http://COUCHDB_USER:COUCHDB_PASSWORD@COUCHDB_HOST:COUCHDB_PORT/nome_do_banco`
- Subir um design doc:\
  `curl -X PUT http://user:pass@host:port/nome_do_banco/_design/minha_view -H "Content-Type: application/json" -d @couchdb/design_docs/minha_view.json`
- Para testar autenticacao basica em R, pode-se usar `httr2::request()` apontando para `paste0(SCHEME, "://", HOST, ":", PORT)`.
