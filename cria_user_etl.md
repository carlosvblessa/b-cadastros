# Usuário `bcad_etl` – ETL somente leitura no CouchDB (b-cadastros)

## 1. Objetivo

Criar o usuário `bcad_etl` no CouchDB para:

- **Extrair dados** dos bancos de réplica do b-cadastros;
- **Sem** permissão de escrita (insert/update/delete) nesses bancos;
- Mantendo o **usuário admin nativo** com acesso total para manutenção/replicação.

Bancos envolvidos:

- `chcnpj_bcadastros_replica`
- `chcpf_bcadastros_replica`
- `chsn_bcadastros_replica`

*(Pode ser estendido para outros, como `chcaepf_bcadastros_replica`.)*

---

## 2. Pré-requisitos

- As variáveis de ambiente mostradas nesta seção devem estar definidas no arquivo `.env`.
- Acesso ao CouchDB com `COUCHDB_SCHEME`, `COUCHDB_HOST` e `COUCHDB_PORT` definidos no `.env`.
- Usuário admin de CouchDB (definido no `local.ini` / `_config/admins`) também definido no `.env`, por exemplo:

```bash
export COUCHDB_SCHEME=http
export COUCHDB_HOST=SEU_HOST_COUCHDB
export COUCHDB_PORT=SUA_PORTA_COUCHDB
export COUCHDB_USER=USUARIO_ADMIN
export COUCHDB_PASSWORD=SENHA_ADMIN

export COUCH_DB_CNPJ=chcnpj_bcadastros_replica
export COUCH_DB_CPF=chcpf_bcadastros_replica
export COUCH_DB_SN=chsn_bcadastros_replica
````

---

## 3. Criação do usuário `bcad_etl`

O usuário de ETL é criado como documento no banco especial `_users`.

```bash
curl -s -k -u "$COUCHDB_USER:$COUCHDB_PASSWORD" \
  -X POST "$COUCHDB_SCHEME://$COUCHDB_HOST:$COUCHDB_PORT/_users" \
  -H "Content-Type: application/json" \
  -d '{
    "_id": "org.couchdb.user:bcad_etl",
    "name": "bcad_etl",
    "password": "SENHA_FORTE_AQUI",
    "roles": ["bcad_etl"],
    "type": "user"
  }'
```

Conferência:

```bash
curl -s -k -u "$COUCHDB_USER:$COUCHDB_PASSWORD" \
  "$COUCHDB_SCHEME://$COUCHDB_HOST:$COUCHDB_PORT/_users/_all_docs?include_docs=true" \
  | jq '.rows[].doc | select(.type=="user") | {name, roles, _id}'
```

Saída esperada (exemplo):

```json
{
  "name": "bcad_etl",
  "roles": ["bcad_etl"],
  "_id": "org.couchdb.user:bcad_etl"
}
```

Sugestão de variáveis de ambiente para o ETL:

```bash
COUCHDB_USER_ETL=bcad_etl
COUCHDB_PASSWORD_ETL=SENHA_FORTE_AQUI
```

---

## 4. Configuração de acesso aos bancos (`_security`)

O documento `/{db}/_security` define quem pode acessar o banco.
Aqui, o objetivo é:

* **Remover** `bcad_etl` de `admins`;
* Manter `bcad_etl` apenas como **member**, com acesso ao banco.

```bash
for db in "$COUCH_DB_CNPJ" "$COUCH_DB_CPF" "$COUCH_DB_SN"; do
  echo "Ajustando _security para $db (sem admins, só members bcad_etl)"
  curl -s -k -u "$COUCHDB_USER:$COUCHDB_PASSWORD" \
    -X PUT "$COUCHDB_SCHEME://$COUCHDB_HOST:$COUCHDB_PORT/$db/_security" \
    -H "Content-Type: application/json" \
    -d '{
      "admins": {
        "names": [],
        "roles": []
      },
      "members": {
        "names": [],
        "roles": ["bcad_etl"]
      }
    }'
done
```

Verificação em um banco (ex.: CNPJ):

```bash
curl -s -k -u "$COUCHDB_USER:$COUCHDB_PASSWORD" \
  "$COUCHDB_SCHEME://$COUCHDB_HOST:$COUCHDB_PORT/$COUCH_DB_CNPJ/_security" | jq
```

Saída esperada:

```json
{
  "admins": {
    "names": [],
    "roles": []
  },
  "members": {
    "names": [],
    "roles": ["bcad_etl"]
  }
}
```

> Observação: isso **não** interfere nos admins globais (role `_admin`) definidos em `/_node/_local/_config/admins`. Esses continuam com poder total em todos os bancos.

---

## 5. Tornando o `bcad_etl` somente leitura (`validate_doc_update`)

Para garantir que o `bcad_etl` **nunca consiga escrever**, é criado um design doc com uma função `validate_doc_update` em cada banco. Ela:

* Libera **admins globais** (`_admin`);
* Bloqueia escrita de qualquer usuário com role `bcad_etl`;
* Não altera o comportamento dos demais usuários.

Criação do design doc:

```bash
for db in "$COUCH_DB_CNPJ" "$COUCH_DB_CPF" "$COUCH_DB_SN"; do
  echo "Criando _design/security_readonly_etl em $db"
  curl -s -k -u "$COUCHDB_USER:$COUCHDB_PASSWORD" \
    -X PUT "$COUCHDB_SCHEME://$COUCHDB_HOST:$COUCHDB_PORT/$db/_design/security_readonly_etl" \
    -H "Content-Type: application/json" \
    -d '{
      "_id": "_design/security_readonly_etl",
      "validate_doc_update": "function (newDoc, oldDoc, userCtx, secObj) { if (userCtx.roles.indexOf(\"_admin\") !== -1) { return; } if (userCtx.roles.indexOf(\"bcad_etl\") !== -1) { throw({forbidden: \"Usuário de ETL é somente leitura neste banco.\"}); } return; }"
    }'
done
```

Verificação (ex.: CNPJ):

```bash
curl -s -k -u "$COUCHDB_USER:$COUCHDB_PASSWORD" \
  "$COUCHDB_SCHEME://$COUCHDB_HOST:$COUCHDB_PORT/$COUCH_DB_CNPJ/_design/security_readonly_etl" | jq
```

---

## 6. Testes de validação

### 6.1. Teste com admin (usuário nativo / `_admin`)

**Objetivo:** garantir que o admin continua podendo escrever.

```bash
curl -s -k -u "$COUCHDB_USER:$COUCHDB_PASSWORD" \
  -X POST "$COUCHDB_SCHEME://$COUCHDB_HOST:$COUCHDB_PORT/$COUCH_DB_CNPJ" \
  -H "Content-Type: application/json" \
  -d '{"teste":"admin consegue salvar?"}'
```

Saída esperada (exemplo):

```json
{"ok":true,"id":"7736ec7954ae6962747a50fd9400d0de","rev":"1-d845f49ef805a9a8027f3e00bf55b1c8"}
```

### Limpeza do documento de teste (opcional)

```bash
curl -s -k -u "$COUCHDB_USER:$COUCHDB_PASSWORD" \
  -X DELETE "$COUCHDB_SCHEME://$COUCHDB_HOST:$COUCHDB_PORT/$COUCH_DB_CNPJ/7736ec7954ae6962747a50fd9400d0de?rev=1-d845f49ef805a9a8027f3e00bf55b1c8"
```

---

### 6.2. Teste com o usuário `bcad_etl`

**Leitura – deve funcionar:**

```bash
curl -s -k -u "bcad_etl:$COUCHDB_PASSWORD_ETL" \
  "$COUCHDB_SCHEME://$COUCHDB_HOST:$COUCHDB_PORT/$COUCH_DB_CNPJ/_all_docs?limit=1" | jq
```

**Escrita – deve falhar com `forbidden`:**

```bash
curl -s -k -u "bcad_etl:$COUCHDB_PASSWORD_ETL" \
  -X POST "$COUCHDB_SCHEME://$COUCHDB_HOST:$COUCHDB_PORT/$COUCH_DB_CNPJ" \
  -H "Content-Type: application/json" \
  -d '{"teste":"etl nao deveria salvar"}'
```

Saída esperada:

```json
{"error":"forbidden","reason":"Usuário de ETL é somente leitura neste banco."}
```

---

## 7. Uso no ETL

A partir daqui, os scripts de ETL devem usar **sempre** o usuário `bcad_etl`:

Exemplo de URL de conexão usando variáveis do `.env`:

```text
${COUCHDB_SCHEME}://bcad_etl:${COUCHDB_PASSWORD_ETL}@${COUCHDB_HOST}:${COUCHDB_PORT}/${COUCH_DB_CNPJ}
```

Vantagens:

* O ETL consegue **ler** os dados de b-cadastros normalmente;
* Não consegue **alterar** nada nos bancos de réplica;
* O usuário admin nativo continua sendo usado apenas para:

  * manutenção,
  * replicação,
  * ajustes de `_security`/design docs.

---

## 8. Resumo

1. Criado usuário `bcad_etl` em `_users` com role `bcad_etl`.
2. Configurado `_security` dos bancos de réplica para:

   * `admins`: vazio
   * `members.roles`: `["bcad_etl"]`
3. Criado design doc `_design/security_readonly_etl` com `validate_doc_update`:

   * Libera `_admin`;
   * Bloqueia escrita de `bcad_etl`.
4. Testes confirmaram:

   * Admin escreve normalmente;
   * `bcad_etl` lê, mas não escreve.

Esse é o padrão recomendado para **usuário técnico de extração (ETL) somente leitura** nos bancos de réplica do b-cadastros.
