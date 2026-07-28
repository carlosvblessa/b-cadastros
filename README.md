# b-cadastros ETL cadastral

Aplicação Python 3.10 que recebe CNPJs, extrai os documentos cadastrais do CouchDB
b-cadastros, aplica as regras A–I de `tmp/regras.md` e grava JSON Lines para consumo por
uma transformação Pentaho. Cada linha da saída principal representa um CNPJ processado
com sucesso.

Os scripts R/PostgreSQL existentes continuam no repositório como legado funcional e não
foram alterados. A aplicação Python não consulta Oracle nem PostgreSQL.

## Escopo e integração com o Pentaho

O Python faz apenas a extração do b-cadastros e as transformações integralmente definidas
no de/para. O Pentaho receberá o JSONL e fará posteriormente os lookups e enriquecimentos
Oracle, como município, subsetor/CNAE, CACEAL e identificadores internos.

As fontes CouchDB confirmadas no ambiente existente são:

| Informação | Banco padrão | Chave do documento |
| --- | --- | --- |
| Cadastro da empresa/raiz e sócios | `chcnpj_bcadastros_replica` | CNPJ raiz, 8 dígitos |
| Estabelecimento | `chcnpj_bcadastros_replica` | CNPJ completo, 14 dígitos |
| Pessoa física | `chcpf_bcadastros_replica` | CPF, 11 dígitos |
| Simples e MEI | `chsn_bcadastros_replica` | CNPJ raiz, 8 dígitos |
| Empresas por responsável | banco CNPJ, endpoint `/_find` | índice `idx-cpf-responsavel` |

### Responsabilidade por campo

| Campo de destino | Responsável nesta etapa | Observação |
| --- | --- | --- |
| `NUM_CNPJ` | Python | Documento como string de 14 dígitos |
| `NOM_RAZAO_SOCIAL` | Python | Cadastro da raiz |
| `NUM_DOC_RESP` | Python | CPF normalizado |
| `COD_TIPDOC_RESP` | Python | `CPF` quando o documento é válido |
| `NOM_RAZAO_SOCIAL_RESP` | Python | Nome consultado no banco CPF |
| `QTD_EMPRESAS_RESP` | Python | Regra I, todas as páginas Mango |
| `IND_OPCAO_SIMPLES` | Python | Regra A |
| `IND_SIMPLES_AUXILIAR` | Python | Regra B |
| `IND_MEI` | Python | Regra A |
| `IND_MEI_AUXILIAR` | Python | Regra B |
| `VAL_CAPITAL_SOCIAL_PJ` | Python | Centavos convertidos com `Decimal` |
| `DTH_INICIO_CNPJ` | Python | Data ISO à meia-noite |
| `DTH_TERMINO_CNPJ` | Python | Regra D |
| `DTH_INICIO_RAIZ` | Python | Mesmo `dataInicioAtividade` definido no de/para |
| `DSC_PORTE` | Python | Regra E |
| `IND_MATRIZ` | Python | `S` para indicador `1`; caso contrário `N` |
| `COD_CNAE` | Python | Código como string numérica |
| `DSC_SITUACAO_CADASTRAL_CNPJ` | Python | Regra C |
| `DTH_SITUACADA_CNPJ` | Python | Data ISO à meia-noite |
| `DSC_MOTIVO_SITUCADA_CNPJ` | Python | Mapeamento local completo da Regra F |
| `NUM_DOC_CONTADOR` | Python | Regra G, com prioridade para PJ |
| `COD_TIPDOC_CONTADOR` | Python | `CNPJ`, `CPF` ou `null` |
| `NOM_RAZAO_SOCIAL_CONT` | Python | Banco CNPJ-raiz ou CPF, conforme contador escolhido |
| `IND_ENDERECO_CONT_FORA_AL` | Python | Usa a UF da mesma fonte escolhida na Regra G |
| `NOM_EMAIL_CAD` | Python | Texto em minúsculas |
| `QTD_SOCIO_RAIZ` | Python | Regra H |
| `NUM_PESSOA_CNPJ`, `NUM_PESSOA_RAIZ` | Oracle/Pentaho | Identificadores internos |
| `NUM_PESSOA_RESP`, `NUM_PESSOA_CONTADOR` | Oracle/Pentaho | Identificadores internos |
| `COD_TIPCONTR`, `DSC_TIPCONTR` | Oracle/Pentaho | Cadastro interno |
| `COD_UNIDMEDI`, `NUM_AREA` | Oracle/Pentaho | Cadastro interno |
| `NOM_MUNICIPIO` | Oracle/Pentaho | Lookup de `codigoMunicipio` |
| `DSC_SUBSETOR` | Oracle/Pentaho | Lookup de CNAE |
| `COD_SITUCADA`, `DSC_SITUACAO_CADASTRAL` | Oracle/Pentaho | CACEAL |
| `IND_ATIVO_SITUCADA`, `SEQ_MOTIALSC_SITUCADA` | Oracle/Pentaho | CACEAL |
| `DSC_MOTIALSC_SITUCADA`, `DTH_ALTERACAO_SITUCADA` | Oracle/Pentaho | CACEAL |
| `IND_ESTEVE_INATIVO` | Oracle/Pentaho | CACEAL |
| `IND_RESP`, `IND_NOTEIRA`, `IND_EMAIL` | Pentaho, pendente de regra | Não implementados: `tmp/regras.md` os marca como ETL sem regra explícita |

Não há cliente Oracle, SQL Oracle nem dependência de Oracle neste pacote.

## Requisitos e instalação

- Linux;
- Python `>=3.10,<3.11` (versão oficial: 3.10);
- acesso HTTP(S) aos três bancos CouchDB;
- usuário CouchDB somente leitura. O procedimento legado está em `cria_user_etl.md`.

Fluxo completo de desenvolvimento:

```bash
python3.10 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

O pacote usa somente três dependências de execução pequenas: `requests`,
`python-dotenv` e `simplejson`. A última preserva `Decimal` como número JSON, sem
convertê-lo em string ou `float`.

## Configuração

Copie o modelo e edite apenas o arquivo local, que é ignorado pelo Git:

```bash
cp .env.example .env
```

| Variável | Padrão/uso |
| --- | --- |
| `COUCHDB_URL` | URL base opcional; alternativa a scheme/host/port |
| `COUCHDB_SCHEME` | `http`; alias legado `COUCH_SCHEME` |
| `COUCHDB_HOST` | obrigatório sem `COUCHDB_URL`; alias `COUCH_HOST` |
| `COUCHDB_PORT` | `5984`; alias `COUCH_PORT` |
| `COUCHDB_USER`, `COUCHDB_PASSWORD` | credenciais obrigatórias; aliases `COUCH_USER`/`COUCH_PASS` |
| `COUCHDB_USER_ETL`, `COUCHDB_PASSWORD_ETL` | se definidos, têm prioridade sobre a conta geral |
| `COUCH_DB_CNPJ`, `COUCH_DB_CPF`, `COUCH_DB_SN` | bancos mostrados na tabela de fontes |
| `COUCHDB_TIMEOUT` | timeout por tentativa, padrão `30` segundos |
| `COUCHDB_MAX_ATTEMPTS` | total de tentativas, padrão `3` |
| `COUCHDB_BACKOFF_FACTOR` | espera exponencial inicial, padrão `0.5` segundo |
| `COUCHDB_PAGE_LIMIT` | página da Regra I, padrão `1000` |
| `COUCHDB_WORKERS` | concorrência máxima, padrão conservador `4` |
| `COUCHDB_VERIFY_SSL` | valida certificado TLS, padrão `true` |
| `COUCH_IDX_RESP_DDOC`, `COUCH_IDX_RESP_NAME` | design doc e nome do índice da Regra I |
| `COUCH_CPF_ID_PREFIX` | prefixo opcional do `_id` de CPF |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING`, `ERROR` ou `CRITICAL` |

Credenciais são enviadas por autenticação básica separada da URL e nunca são incluídas
nas mensagens de erro. Para HTTPS com certificado privado, prefira instalar a CA no
sistema; desative `COUCHDB_VERIFY_SSL` apenas em ambiente controlado.

## Entrada

O arquivo recebe um CNPJ por linha, com ou sem máscara. Linhas vazias são ignoradas,
duplicidades são removidas mantendo a primeira ocorrência e documentos com tamanho
diferente de 14 são registrados como erros isolados. Não há validação de dígitos
verificadores.

```text
00.123.456/0001-99
98765432000100
```

Arquivo:

```bash
python -m bcadastros_etl \
  --input examples/cnpjs.txt \
  --output resultado.jsonl \
  --errors erros.jsonl
```

Entrada padrão, útil para o Pentaho:

```bash
cat cnpjs.txt | python -m bcadastros_etl \
  --output resultado.jsonl \
  --errors erros.jsonl
```

Também é possível escrever os dados em `stdout` omitindo `--output` ou informando
`--output -`. Logs continuam em `stderr`. `--errors` sempre exige um arquivo para não
misturar falhas com os dados destinados ao Pentaho.

## Saída e erros

A saída é UTF-8, contém um objeto por linha, usa `ensure_ascii=False`, mantém ordem
estável de chaves e representa ausências com `null`. Datas válidas saem como
`YYYY-MM-DDT00:00:00`, documentos como strings, indicadores como `S`/`N` e capital
social como número JSON.

Exemplo reduzido:

```json
{"NUM_CNPJ":"00123456000199","NOM_RAZAO_SOCIAL":"EMPRESA ÁRVORE LTDA","IND_OPCAO_SIMPLES":"S"}
{"NUM_CNPJ":"98765432000100","NOM_RAZAO_SOCIAL":"OUTRA EMPRESA SA","IND_OPCAO_SIMPLES":"N"}
```

O arquivo de erros é separado:

```json
{"cnpj":"00123456000199","etapa":"consulta_cnpj","tipo_erro":"CouchDBHTTPError","mensagem":"Resposta HTTP 503 apos 3 tentativas"}
```

Retentativas são feitas para timeout, falha de conexão, HTTP 429 e HTTP 5xx, com espera
progressiva e respeito ao `Retry-After` numérico. Um erro de registro não interrompe os
demais.

| Código de saída | Significado |
| --- | --- |
| `0` | lote concluído, inclusive quando há erros isolados |
| `2` | falha estrutural conhecida: configuração, entrada ou criação de saída |
| `3` | falha estrutural inesperada |

## Regras e decisões de implementação

- Textos do JSON são aparados e convertidos para maiúsculas, preservando acentos;
  somente e-mail é convertido para minúsculas.
- CNPJ/CPF nunca são convertidos para inteiro ou validados por dígito verificador.
- `PeriodoSimples` e `PeriodoMEI` aceitam `null`, string JSON, objeto ou lista. O CouchDB
  real usa listas; nesse caso é escolhido o período mais recente por `Inicio`, seguindo o
  comportamento útil do R. As regras A/B usam apenas `Inicio` e `Fim`, como definido em
  `tmp/regras.md`; `Cancelado` e `Anulado` não acrescentam condições não documentadas.
- JSON inválido de período gera aviso e equivale a período vazio.
- A Regra I encerra com segurança em página curta, ausência de `docs`, bookmark vazio ou
  repetido. Respostas estruturalmente inválidas viram erro isolado do CNPJ.
- Ausência de documento de Simples/MEI significa indicadores `N`. Pessoa ou contador não
  encontrado gera nome `null`. A ausência do cadastro-raiz ou do estabelecimento impede
  um registro completo e é escrita no arquivo de erros.
- `DTH_INICIO_RAIZ` replica `dataInicioAtividade` do estabelecimento porque essa é a regra
  explícita no de/para; não foi inventada uma consulta para descobrir a primeira filial.
- Há uma inconsistência aritmética no exemplo de capital em `tmp/regras.md`:
  `00000001800000 / 100 = 18000.00`. Para representar `180000.00` em centavos, a origem
  deve ser `00000018000000`. A implementação segue a regra textual e o R (`Decimal / 100`),
  preservando zeros à esquerda sem deixá-los alterar o valor.

## Arquitetura

```text
bcadastros_etl/
  cli.py                 argumentos e códigos de saída
  config.py              ambiente/.env e validação
  couchdb_client.py      HTTP, cache, retentativas e paginação
  extractors.py          composição raiz/estabelecimento/SN/CPF
  transformers.py        regras A–H e transformações puras
  mappings.py            domínios locais, incluindo a Regra F
  models.py              ordem e tipos do registro de saída
  input_reader.py        normalização/deduplicação da entrada
  jsonl_writer.py        serialização JSONL/Decimal
  application.py         concorrência limitada e ordem estável
tests/
  unit/                  regras e HTTP simulados
  integration/           pipeline completo com dublês locais
examples/                massa sintética de entrada e saída
```

Cada worker reutiliza uma sessão HTTP. Consultas repetidas a CPF, CNPJ-raiz, contador e
quantidade de empresas são mantidas em cache durante o lote. A janela de futures é
limitada e a saída respeita a ordem dos CNPJs válidos de entrada.

## Testes e qualidade

```bash
pytest
ruff check .
mypy bcadastros_etl
```

Os testes não acessam CouchDB real. A massa sintética está em `examples/cnpjs.txt` e
`examples/resultado_esperado.jsonl`; ela documenta o formato, não pressupõe que esses
CNPJs existam no ambiente.
