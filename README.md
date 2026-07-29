# b-cadastros ETL cadastral

Aplicação de linha de comando em Python 3.10 que recebe CNPJs, consulta dados cadastrais
no CouchDB/b-cadastros, aplica as regras A–I de [`tmp/regras.md`](tmp/regras.md) e gera
JSON Lines (JSONL) para consumo posterior por uma transformação Pentaho.

O Python não consulta Oracle nem PostgreSQL. Os scripts R, SQL e utilitários legados
permanecem no repositório, mas não fazem parte do pacote `bcadastros_etl`.

## Escopo e fluxo

Para cada CNPJ válido, a aplicação:

1. normaliza e deduplica a entrada, mantendo a primeira ocorrência;
2. obtém a raiz pelas oito primeiras posições;
3. consulta a projeção da raiz, o estabelecimento completo e os períodos de Simples/MEI;
4. consulta nome e quantidade de empresas do responsável, quando há CPF válido;
5. seleciona o contador PJ antes do PF e consulta seu nome;
6. aplica as regras funcionais locais;
7. grava o resultado em ordem de entrada ou isola o erro daquele registro.

As consultas confirmadas no desenho atual são:

| Informação | Banco padrão | Identificador |
| --- | --- | --- |
| Empresa/raiz | `chcnpj_bcadastros_replica` | raiz de 8 posições |
| Estabelecimento | `chcnpj_bcadastros_replica` | CNPJ de 14 posições |
| Pessoa física | `chcpf_bcadastros_replica` | CPF de 11 dígitos |
| Simples e MEI | `chsn_bcadastros_replica` | raiz de 8 posições |
| Empresas por responsável | banco CNPJ, `/_find` | índice `idx-cpf-responsavel` |

Raízes e CNPJs podem conter letras. CPF continua exclusivamente numérico.

## Compatibilidade

- Python oficial: `>=3.10,<3.11`;
- CNPJ antigo, inteiramente numérico;
- CNPJ alfanumérico: 14 posições, letras ASCII ou dígitos nas 12 primeiras e dígitos
  verificadores nas duas últimas;
- entrada mascarada no formato `XX.XXX.XXX/XXXX-XX` ou sem máscara;
- letras minúsculas são convertidas para maiúsculas;
- zeros à esquerda são preservados;
- não há validação matemática dos dígitos verificadores.

Exemplos aceitos:

```text
12.345.678/0001-95 -> 12345678000195
12.345.678/000A-08 -> 12345678000A08
AA.345.678/0003-29 -> AA345678000329
```

Caracteres inesperados e máscaras fora do formato são rejeitados. A aplicação não usa
remoção indiscriminada de não dígitos para CNPJ. O contador PJ segue a mesma normalização;
contador PF e responsável usam uma rotina separada e exclusivamente numérica.

## Instalação

Não versione `.venv`:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

As dependências de execução são `requests`, `python-dotenv` e `simplejson`. Não há
cliente Oracle, framework web, biblioteca assíncrona ou cache externo.

## Configuração

Em produção, prefira variáveis de ambiente fornecidas pelo orquestrador. Um arquivo
dotenv só é lido quando indicado explicitamente; assim, a execução não depende do
diretório corrente:

```bash
cp .env.example config.env

python -m bcadastros_etl \
  --env-file /caminho/absoluto/config.env \
  --input /dados/cnpjs.txt \
  --output /dados/resultado.jsonl \
  --errors /dados/erros.jsonl
```

| Variável | Padrão/regra |
| --- | --- |
| `COUCHDB_URL` | URL HTTP(S); alternativa a scheme/host/port |
| `COUCHDB_SCHEME` | `http`; aceita alias legado `COUCH_SCHEME` |
| `COUCHDB_HOST` | obrigatório sem URL; alias `COUCH_HOST` |
| `COUCHDB_PORT` | `5984`, inteiro entre 1 e 65535; alias `COUCH_PORT` |
| `COUCHDB_USER`, `COUCHDB_PASSWORD` | obrigatórios e não vazios |
| `COUCHDB_USER_ETL`, `COUCHDB_PASSWORD_ETL` | prioridade sobre a conta geral |
| `COUCH_DB_CNPJ` | `chcnpj_bcadastros_replica` |
| `COUCH_DB_CPF` | `chcpf_bcadastros_replica` |
| `COUCH_DB_SN` | `chsn_bcadastros_replica` |
| `COUCHDB_TIMEOUT` | `30`, segundos positivos por tentativa |
| `COUCHDB_MAX_ATTEMPTS` | `3`, total positivo de tentativas |
| `COUCHDB_BACKOFF_FACTOR` | `0.5`, espera exponencial não negativa |
| `COUCHDB_PAGE_LIMIT` | `1000`, tamanho positivo da página da Regra I |
| `WORKERS` | `4`, inteiro maior ou igual a 1 |
| `COUCHDB_VERIFY_SSL` | `true` |
| `COUCH_IDX_RESP_DDOC` | `_design/idx_cpf_responsavel` |
| `COUCH_IDX_RESP_NAME` | `idx-cpf-responsavel` |
| `COUCH_CPF_ID_PREFIX` | prefixo opcional do `_id` de CPF |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING`, `ERROR` ou `CRITICAL` |

Valores obrigatórios contendo apenas espaços são rejeitados. Credenciais ficam fora da
URL e nunca são incluídas em mensagens ou métricas.

### Concorrência

A precedência é:

1. `--workers`;
2. variável `WORKERS`;
3. padrão fixo `4`.

Não existe ajuste por `os.cpu_count()` nem limitação silenciosa do valor solicitado. A
quantidade efetiva é registrada no início da execução. Quatro workers produziram o
melhor resultado geral no teste comparativo original com 4, 8, 12 e 16 workers. Esse
valor deve ser recalibrado em outro servidor ou após mudanças no CouchDB, rede ou código.

```bash
# Usa 4
python -m bcadastros_etl --input cnpjs.txt --output resultado.jsonl

# CLI prevalece sobre WORKERS
export WORKERS=8
python -m bcadastros_etl \
  --input cnpjs.txt \
  --output resultado.jsonl \
  --workers 12
```

### Caches limitados

Estabelecimentos nunca entram em cache. Eles normalmente são lidos uma vez porque a
entrada já foi deduplicada. Os dados reutilizáveis usam caches LRU independentes,
thread-safe e limitados:

| Variável | Padrão | Conteúdo compacto |
| --- | ---: | --- |
| `CACHE_ROOT_MAXSIZE` | 20000 | nome, CPF responsável, capital, porte e quantidade de sócios |
| `CACHE_SIMPLES_MAXSIZE` | 20000 | períodos de Simples e MEI |
| `CACHE_PERSON_MAXSIZE` | 10000 | nomes de pessoas físicas |
| `CACHE_ACCOUNTANT_MAXSIZE` | 10000 | nomes de contadores por tipo/documento |
| `CACHE_RESPONSIBLE_COUNT_MAXSIZE` | 10000 | contagens completas da Regra I |

Os limites são quantidades de entradas, aceitam zero para desabilitar o cache e nunca
podem ser negativos. Respostas não encontradas (`None`) também podem ser armazenadas
para evitar consultas repetidas.

Ao fim do lote, cada cache registra somente `hits`, `misses`, inclusões, remoções LRU,
tamanho atual e máximo. Documentos e credenciais não aparecem no log.

## Entrada

O arquivo ou `stdin` recebe um CNPJ por linha:

```text
00.123.456/0001-99
12.345.678/000A-08
AA345678000329
```

Linhas vazias são ignoradas. Formas mascarada e não mascarada do mesmo CNPJ são
deduplicadas. Uma linha inválida vira registro isolado no JSONL de erros e não interrompe
o lote.

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

Omitir `--output` ou usar `--output -` escreve dados em `stdout`. Logs continuam em
`stderr`. `--errors` exige um arquivo para impedir mistura entre erros e dados.

## Saída JSONL

Cada linha bem-sucedida é um objeto JSON UTF-8 com 26 campos em ordem estável.
Ausências usam `null`; datas usam `YYYY-MM-DDT00:00:00`; documentos permanecem strings;
indicadores usam `S`/`N`; `Decimal` é serializado como número JSON.

Exemplo sintético e anonimizado:

```json
{"NUM_CNPJ":"AA345678000329","NOM_RAZAO_SOCIAL":"EMPRESA EXEMPLO LTDA","NUM_DOC_RESP":null,"COD_TIPDOC_RESP":"CPF","QTD_EMPRESAS_RESP":0,"IND_OPCAO_SIMPLES":"N","IND_MEI":"N"}
```

`COD_TIPDOC_RESP` é sempre `"CPF"`, inclusive quando `NUM_DOC_RESP` é `null`. Essa
decisão segue literalmente `tmp/regras.md`, fonte funcional principal.

Textos cadastrais são aparados e convertidos para maiúsculas, preservando acentos. E-mail
é a única exceção e sai em minúsculas. O formato completo está em
[`examples/resultado_esperado.jsonl`](examples/resultado_esperado.jsonl).

## Erros, retentativas e códigos de saída

Erros isolados são gravados separadamente:

```json
{"cnpj":"AA345678000329","etapa":"consulta_cnpj","tipo_erro":"CouchDBHTTPError","mensagem":"Resposta HTTP 503 apos 3 tentativas"}
```

Timeout, falha de conexão, HTTP 429 e HTTP 5xx recebem retentativas com espera
exponencial e suporte a `Retry-After` numérico.

A Regra I só grava uma contagem quando percorre todas as páginas. Depois de uma página
cheia, bookmark ausente, vazio, repetido ou já utilizado lança
`CouchDBPaginationError`; `docs` ausente/nulo, JSON inválido e estrutura inesperada
também falham. Nesses casos, o CNPJ vai para o JSONL de erros e nenhuma contagem parcial
chega à saída principal. Uma página menor que o limite encerra normalmente.

| Código | Significado |
| ---: | --- |
| `0` | lote concluído, inclusive com erros isolados |
| `2` | falha estrutural conhecida de configuração, entrada ou saída |
| `3` | falha estrutural inesperada |

Falhas de um estabelecimento ou raiz inexistente continuam isoladas e não alteram o
código zero do restante do lote.

### Reprocessamento

Depois de corrigir a causa (documento replicado, índice, rede ou configuração), gere uma
nova lista a partir dos erros:

```bash
jq -r 'select(.etapa != "validacao_entrada") | .cnpj' erros.jsonl \
  > cnpjs_reprocessar.txt

python -m bcadastros_etl \
  --input cnpjs_reprocessar.txt \
  --output resultado_reprocessado.jsonl \
  --errors erros_reprocessados.jsonl
```

Erros de `validacao_entrada` devem ser corrigidos antes do reprocessamento. O resultado
reprocessado deve ser conciliado pelo `NUM_CNPJ` no fluxo Pentaho, sem concatenar linhas
às cegas.

## Regras e decisões funcionais

- Regras A e B aceitam período como string JSON, objeto ou lista; em lista, escolhem o
  período com `Inicio` mais recente.
- JSON de período inválido gera aviso e equivale a período vazio.
- Regras C–F usam os mapeamentos de `tmp/regras.md`.
- Regra G privilegia a origem `contadorPJ`, aplica normalização alfanumérica de CNPJ e só
  usa `contadorPF` quando PJ está ausente.
- Regra H armazena apenas a quantidade de sócios na projeção da raiz.
- Regra I consulta somente `["_id"]` e exige paginação completa.
- `DTH_INICIO_RAIZ` replica `dataInicioAtividade`, conforme o de/para.
- Capital social usa `Decimal(capitalSocial) / 100`. O exemplo aritmético de
  `tmp/regras.md` é inconsistente: `00000001800000 / 100` resulta em `18000.00`;
  a implementação segue a regra textual e o comportamento legado.

## Integração futura com Pentaho/Oracle

O JSONL é a fronteira entre Python e Pentaho. Continuam fora deste pacote:

- `NUM_PESSOA_CNPJ`, `NUM_PESSOA_RAIZ`, `NUM_PESSOA_RESP`,
  `NUM_PESSOA_CONTADOR`;
- `COD_TIPCONTR`, `DSC_TIPCONTR`, `COD_UNIDMEDI`, `NUM_AREA`;
- lookup de município e subsetor/CNAE;
- campos CACEAL `COD_SITUCADA`, `DSC_SITUACAO_CADASTRAL`,
  `IND_ATIVO_SITUCADA`, `SEQ_MOTIALSC_SITUCADA`,
  `DSC_MOTIALSC_SITUCADA`, `DTH_ALTERACAO_SITUCADA`,
  `IND_ESTEVE_INATIVO`;
- `IND_RESP`, `IND_NOTEIRA` e `IND_EMAIL`, ainda sem regra explícita.

Não existe dependência Oracle no pacote Python.

## Arquitetura

```text
bcadastros_etl/
  normalization.py   normalização separada de CNPJ e documentos numéricos
  cache.py           LRU genérico, thread-safe, limitado e com métricas
  config.py          ambiente, --env-file e validação
  couchdb_client.py  HTTP, projeções, caches, retentativas e Regra I
  extractors.py      composição das consultas por CNPJ
  transformers.py   regras A–H e transformações puras
  mappings.py        domínios locais
  models.py          projeções compactas e modelos da fronteira JSONL
  input_reader.py    validação e deduplicação
  jsonl_writer.py    serialização JSONL/Decimal
  application.py     janela concorrente limitada e ordem estável
  cli.py             argumentos, observabilidade e códigos de saída
tests/
  unit/              normalização, regras, cache, HTTP e CLI
  integration/       pipeline, isolamento e servidor HTTP local
examples/            entrada e saída sintéticas
```

Cada worker reutiliza uma sessão HTTP. A janela de futures é limitada a duas vezes a
quantidade de workers e a saída preserva a ordem original.

## Testes e qualidade

Os testes não acessam CouchDB real:

```bash
ruff check .
ruff format --check .
mypy bcadastros_etl
pytest
```

Ruff está configurado para Python 3.10, PEP 8, imports, modernização compatível, erros
comuns e docstrings PEP 257 em convenção Google. Mypy verifica todas as funções tipadas
do pacote.

## Desempenho e memória

Referência do lote anterior:

- 576.922 entradas;
- 576.913 sucessos e 9 erros isolados;
- 1h09min50s, aproximadamente 137,7 CNPJs/s;
- RSS máximo aproximado de 9,5 GiB.

O desenho atual remove o fator principal de crescimento: nenhum dos 576.922 documentos
de estabelecimento permanece em memória, e cada cache reutilizável tem tamanho máximo
fixo.

Uma medição sintética local em Python 3.10, com caches de raiz e Simples reduzidos
deliberadamente a 1.000 entradas, processou 100.000 CNPJs e gerou documentos descartáveis
de aproximadamente 8–18 KiB por consulta, sem rede:

- 100.000 sucessos, zero erros e 300.000 consultas simuladas;
- 16,566 s e 6.036,4 CNPJs/s;
- RSS máximo de 34.936 KiB (aproximadamente 34,1 MiB);
- cache de raiz: 1.000/1.000 entradas e 99.000 remoções LRU;
- cache de Simples: 1.000/1.000 entradas e 99.000 remoções LRU;
- nenhuma entrada de estabelecimento retida.

Essa massa força 100.000 raízes distintas e comprova que a retenção dos caches permanece
no limite configurado, em vez de acompanhar o total. A vazão sintética não é comparável
aos 137,7 CNPJs/s reais porque não inclui CouchDB nem rede. Uma nova medição com o lote
real ainda é necessária para comparar RSS, taxa de acerto e vazão no mesmo servidor.

## Limitações conhecidas

- não valida os dígitos verificadores de CNPJ/CPF;
- não mede latência, RSS ou vazão do CouchDB real durante testes automatizados;
- concorrência e tamanhos de cache podem exigir nova calibração por ambiente;
- período inválido é tratado como vazio com aviso, seguindo o contrato atual;
- enriquecimentos Oracle/Pentaho permanecem deliberadamente pendentes;
- reprocessamento e conciliação final ainda precisam ser incorporados à transformação
  Pentaho.
