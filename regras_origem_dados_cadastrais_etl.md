# De -> Para (JSON -> `ADMODS001.ODS_BAIXAPOROF_CADASTRO`)

**Padrão de normalização de texto no ETL**

* Para todos os campos textuais originados do JSON: aplicar `TO_UPPER(...)`.
* Exceção: **e-mail** (`NOM_EMAIL_CAD`) deve ser `TO_LOWER(...)`.
* Campos numéricos/datas seguem as regras específicas (conversão/parse).

> Observação: itens marcados como **ORACLE** são obtidos de tabelas Oracle (não do JSON). Itens marcados como **ETL** são derivados por regra no ETL.

| Origem (JSON/ETL/ORACLE)                                         | Coluna na tabela              | Regra / transformação                                                                                      |
| ---------------------------------------------------------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `ORACLE`                                                         | `NUM_PESSOA_CNPJ`             | ORACLE                                                                                                     |
| `ORACLE`                                                         | `NUM_PESSOA_RAIZ`             | ORACLE                                                                                                     |
| `cnpj`                                                           | `NUM_CNPJ`                    | Copiar como string, preservando zeros à esquerda.                                                          |
| `nomeEmpresarial`                                                | `NOM_RAZAO_SOCIAL`            | Copiar e aplicar `TO_UPPER`.                                                                               |
| `ORACLE`                                                         | `NUM_PESSOA_RESP`             | ORACLE (avaliar necessidade: pode haver divergência com base SEFAZ; verificar uso real no ETL).            |
| `cpfResponsavel`                                                 | `NUM_DOC_RESP`                | Copiar como string (CPF), preservando zeros à esquerda.                                                    |
| *(fixo)*                                                         | `COD_TIPDOC_RESP`             | Preencher com `'CPF'`                                                      |
| `cpfResponsavel`                                                 | `NOM_RAZAO_SOCIAL_RESP`       | Consultar b-cadastros por CPF e preencher com o nome (aplicar `TO_UPPER`).                                 |
| `ORACLE`                                                         | `QTD_EMPRESAS_RESP`           | ORACLE                                                                                                     |
| `ETL`                                                            | `IND_RESP`                    | ETL                                                                                                        |
| `ETL`                                                            | `IND_NOTEIRA`                 | ETL                                                                                                        |
| `PeriodoSimples`                                                 | `IND_OPCAO_SIMPLES`           | **Consultar Regra A (abaixo)**                                                                             |
| `PeriodoSimples` + `situacaoCadastral` + `dataSituacaoCadastral` | `IND_SIMPLES_AUXILIAR`        | **Consultar Regra B (abaixo)**                                                                             |
| `PeriodoMEI`                                                     | `IND_MEI`                     | **Consultar Regra A (abaixo)**                                                                             |
| `PeriodoMEI` + `situacaoCadastral` + `dataSituacaoCadastral`     | `IND_MEI_AUXILIAR`            | **Consultar Regra B (abaixo)**                                                                             |
| `ORACLE`                                                         | `COD_TIPCONTR`                | ORACLE                                                                                                     |
| `ORACLE`                                                         | `DSC_TIPCONTR`                | ORACLE                                                                                                     |
| `capitalSocial`                                                  | `VAL_CAPITAL_SOCIAL_PJ`       | Valor em centavos sem separador ⇒ `TO_NUMBER(capitalSocial) / 100`. Ex.: `"00000001800000"` -> `180000.00`. |
| `dataInicioAtividade`                                            | `DTH_INICIO_CNPJ`             | `TO_TIMESTAMP(dataInicioAtividade,'YYYYMMDD')`.                                                            |
| `situacaoCadastral` + `dataSituacaoCadastral`                    | `DTH_TERMINO_CNPJ`            | **Consultar Regra D (abaixo)**                                                                             |
| `dataInicioAtividade`                                            | `DTH_INICIO_RAIZ`             | `TO_TIMESTAMP(dataInicioAtividade,'YYYYMMDD')`.                                                            |
| `porteEmpresa`                                                   | `DSC_PORTE`                   | **Consultar Regra E (abaixo)**                                                                             |
| `ORACLE`                                                         | `COD_UNIDMEDI`                | ORACLE                                                                                                     |
| `ORACLE`                                                         | `NUM_AREA`                    | ORACLE                                                                                                     |
| `codigoMunicipio`                                                | `NOM_MUNICIPIO`               | Lookup em tabela Oracle (código município -> descrição; aplicar `TO_UPPER`).                                |
| `indicadorMatriz`                                                | `IND_MATRIZ`                  | Normalizar: `'S'` se `indicadorMatriz='1'`, senão `'N'`.                                                   |
| `cnaeFiscal`                                                     | `COD_CNAE`                    | Copiar como string numérica.                                                                               |
| `cnaeFiscal`                                                     | `DSC_SUBSETOR`                | Lookup em tabela Oracle (CNAE -> descrição; aplicar `TO_UPPER`).                                            |
| `ORACLE`                                                         | `COD_SITUCADA`                | ORACLE (CACEAL)                                                                                            |
| `ORACLE`                                                         | `DSC_SITUACAO_CADASTRAL`      | ORACLE (CACEAL)                                                                                            |
| `ORACLE`                                                         | `IND_ATIVO_SITUCADA`          | ORACLE (CACEAL)                                                                                            |
| `ORACLE`                                                         | `SEQ_MOTIALSC_SITUCADA`       | ORACLE (CACEAL)                                                                                            |
| `ORACLE`                                                         | `DSC_MOTIALSC_SITUCADA`       | ORACLE (CACEAL)                                                                                            |
| `ORACLE`                                                         | `DTH_ALTERACAO_SITUCADA`      | ORACLE (CACEAL)                                                                                            |
| `situacaoCadastral`                                              | `DSC_SITUACAO_CADASTRAL_CNPJ` | **Consultar Regra C (abaixo)**                                                                             |
| `dataSituacaoCadastral`                                          | `DTH_SITUACADA_CNPJ`          | `TO_TIMESTAMP(dataSituacaoCadastral,'YYYYMMDD')`.                                                          |
| `motivoSituacao`                                                 | `DSC_MOTIVO_SITUCADA_CNPJ`    | Verificar se existe tabela Oracle; se não existir (ou lookup falhar), **Consultar Regra F (abaixo)**.      |
| `ORACLE`                                                         | `NUM_PESSOA_CONTADOR`         | ORACLE (avaliar necessidade: pode haver divergência com base SEFAZ; verificar uso real no ETL).            |
| `contadorPF` / `contadorPJ`                                      | `NUM_DOC_CONTADOR`            | **Consultar Regra G (abaixo)**                                                                             |
| `contadorPF` / `contadorPJ`                                      | `COD_TIPDOC_CONTADOR`         | **Consultar Regra G (abaixo)**                                                                             |
| `contadorPF` / `contadorPJ` + consulta b-cadastros               | `NOM_RAZAO_SOCIAL_CONT`       | **Consultar Regra G (abaixo)**                                                                             |
| `ufCrcContadorPF` / `ufCrcContadorPJ`                            | `IND_ENDERECO_CONT_FORA_AL`   | **Consultar Regra G (abaixo)**                                                                             |
| `email`                                                          | `NOM_EMAIL_CAD`               | Copiar e aplicar `TO_LOWER`.                                                                               |
| `socios`                                                         | `QTD_SOCIO_RAIZ`              | **Consultar Regra H (abaixo)**                                                                             |
| `ETL`                                                            | `IND_EMAIL`                   | ETL                                                                                                        |
| `ORACLE`                                                         | `IND_ESTEVE_INATIVO`          | ORACLE (CACEAL)                                                                                            |


## Regra A - Indicadores principais (`IND_OPCAO_SIMPLES` e `IND_MEI`)

**Entradas:**

* Para Simples: `PeriodoSimples.Inicio`, `PeriodoSimples.Fim`
* Para MEI: `PeriodoMEI.Inicio`, `PeriodoMEI.Fim`
  (onde `Periodo*` é um **JSON em string**, que precisa ser parseado)

**Regra (vale para cada um dos períodos, separadamente):**

1. Se **`Inicio` está preenchido** **e** **`Fim` está vazio/nulo/não preenchido** ⇒ indicador principal = **'S'**
2. Caso contrário (ou seja: **nenhuma data preenchida** *ou* **as duas datas preenchidas**) ⇒ indicador principal = **'N'**

> Observação operacional: aqui "vazio/nulo/não preenchido" inclui `NULL`, string vazia `''` e campo inexistente no JSON parseado.

---

## Regra B - Indicadores auxiliares (`IND_SIMPLES_AUXILIAR` e `IND_MEI_AUXILIAR`)

**Objetivo:** reter a **mesma regra do indicador principal** (Regra A), **porém** marcar **'S'** em situações em que a RFB "encerra" o período por mudança cadastral, para sinalizar que **houve opção enquanto operava**.

**Entradas adicionais:**

* `situacaoCadastral` {`01`, `02`, `03`, `04`, `08`}

  * `01` Nula, `02` Ativa, `03` Suspensa, `04` Inapta, `08` Baixada
* `dataSituacaoCadastral` (formato `YYYYMMDD`)
* `Periodo*.Fim` (datetime no JSON do período)

### B1) Regra base

* Primeiro calcule o indicador principal correspondente via **Regra A**.

  * Se principal = **'S'**, então o auxiliar também é **'S'** (sem mais testes).
  * Se principal = **'N'**, aplique a regra de exceção abaixo.

### B2) Regra de exceção (situações que "finalizam" o período)

Se **todas** as condições forem verdadeiras:

1. `situacaoCadastral` está em **{`01`, `04`, `08`}** (Nula, Inapta, Baixada); **e**
2. `Periodo*.Fim` está preenchido; **e**
3. `dataSituacaoCadastral` **coincide** com a data de `Periodo*.Fim`

   * Comparação por **data** (ignorando hora):
     `YYYYMMDD(dataSituacaoCadastral) == YYYYMMDD(Periodo*.Fim)`

⇒ então o indicador auxiliar = **'S'**.

Caso contrário ⇒ auxiliar = **'N'**.

---

### Definição de "coincidir"

* Converter `dataSituacaoCadastral` (`YYYYMMDD`) para data.
* Converter `Periodo*.Fim` (ex.: `2025-12-31T00:00:00`) para data.
* Comparar apenas a parte da data (dia/mês/ano), desconsiderando horário.



## Regra C - Mapeamento de `situacaoCadastral` (código -> descrição)

**Objetivo:** derivar uma descrição textual padronizada (ex.: `DSC_SITUACAO_CADASTRAL_CNPJ`) a partir do campo `situacaoCadastral`.

1. **Entrada:** `situacaoCadastral` (string ou número).
2. **Normalização:**

   * Se `situacaoCadastral` for **NULL**, inexistente, vazio (`''`) ou só espaços -> resultado = **NULL**.
   * Converter para string e aplicar `trim`.
   * Se vier com **1 dígito** (ex.: `1`, `2`) ⇒ padronizar para **2 dígitos**, prefixando `0` (ex.: `1` -> `01`).
3. **Mapeamento (tabela de decisão):**

   * `01` ⇒ **NULA**
   * `02` ⇒ **ATIVA**
   * `03` ⇒ **SUSPENSA**
   * `04` ⇒ **INAPTA**
   * `05` ⇒ **ATIVA NAO REGULAR**
   * `08` ⇒ **BAIXADO**
   * Qualquer outro valor ⇒ **NAO INFORMADA**

**Saída:** string com a descrição da situação cadastral (ou NULL quando não houver código válido).

## Regra D — Término do CNPJ (`DTH_TERMINO_CNPJ`) com base em `situacaoCadastral` e `dataSituacaoCadastral`

**Objetivo:** preencher `DTH_TERMINO_CNPJ` quando a empresa entrar em situação que caracteriza encerramento/inaptidão, usando a data da situação cadastral.

1. **Entradas:**

   * `situacaoCadastral` (código)
   * `dataSituacaoCadastral` (formato `YYYYMMDD`)

2. **Condição para término:**

   * Se `situacaoCadastral` **normalizada para 2 dígitos** estiver em **{`01`, `04`, `08`}** *(Nula, Inapta, Baixada)* **e**
   * `dataSituacaoCadastral` estiver **preenchida e válida** (`YYYYMMDD`)

   ⇒ então `DTH_TERMINO_CNPJ = TO_TIMESTAMP(dataSituacaoCadastral, 'YYYYMMDD')` *(ou `TO_DATE(... )` se o padrão do ETL for data sem hora).*

3. **Caso contrário:**

   * `DTH_TERMINO_CNPJ = NULL`

4. **Regras de validação/normalização:**

   * Se `situacaoCadastral` for NULL/vazio ⇒ não termina (NULL).
   * Se `situacaoCadastral` vier com 1 dígito (`1`, `4`, `8`) ⇒ transformar em `01`, `04`, `08`.
   * Se `dataSituacaoCadastral` for NULL/vazio/inválida ⇒ não termina (NULL), mesmo que a situação esteja em `{01,04,08}`.

**Interpretação:** a data de término do CNPJ é a própria data em que a situação cadastral passou a ser Nula/Inapta/Baixada.


## Regra E - Mapeamento de `porteEmpresa` (código -> `DSC_PORTE`)

**Objetivo:** derivar `DSC_PORTE` a partir do campo `porteEmpresa`.

1. **Entrada:** `porteEmpresa` (string ou número).
2. **Normalização:**

   * Se `porteEmpresa` for **NULL**, inexistente, vazio (`''`) ou só espaços ⇒ `DSC_PORTE = NULL`.
   * Converter para string e aplicar `trim`.
   * Se vier com **1 dígito** (ex.: `1`, `3`, `5`) ⇒ padronizar para **2 dígitos**, prefixando `0` (ex.: `1` -> `01`).
3. **Mapeamento (tabela de decisão):**

   * `01` ⇒ `DSC_PORTE = 'MICRO-EMPRESA'`
   * `03` ⇒ `DSC_PORTE = 'EMPRESA PEQUENO PORTE'`
   * `05` ⇒ `DSC_PORTE = 'DEMAIS'`
   * Qualquer outro valor ⇒ `DSC_PORTE = 'NAO INFORMADO'`

**Saída:** `DSC_PORTE` (string) ou NULL quando não houver código válido.

## Regra F - Mapeamento de `motivoSituacao` (código -> `DSC_MOTIVO_SITUCADA_CNPJ`)

**Objetivo:** preencher `DSC_MOTIVO_SITUCADA_CNPJ` a partir do campo `motivoSituacao` quando **não existir tabela de domínio** na base Oracle (ou quando o lookup não retornar resultado).

1. **Entrada:** `motivoSituacao` (string ou número).
2. **Normalização:**

   * Se `motivoSituacao` for **NULL**, inexistente, vazio (`''`) ou só espaços ⇒ `DSC_MOTIVO_SITUCADA_CNPJ = NULL`.
   * Converter para string e aplicar `trim`.
   * Se vier com **1 dígito** (ex.: `1`, `2`, `6`) ⇒ **prefixar `0`** para ficar com 2 dígitos (`01`, `02`, `06`).
3. **Mapeamento (tabela de decisão):**
   Definir `DSC_MOTIVO_SITUCADA_CNPJ` conforme o código normalizado:

* `00` -> **SEM MOTIVO**
* `01` -> **EXTINCAO POR ENCERRAMENTO LIQUIDACAO VOLUNTARIA**
* `02` -> **INCORPORACAO**
* `03` -> **FUSAO**
* `04` -> **CISAO TOTAL**
* `05` -> **ENCERRAMENTO DA FALENCIA**
* `06` -> **ENCERRAMENTO DA LIQUIDACAO**
* `07` -> **ELEVACAO A MATRIZ**
* `08` -> **TRANSPASSE**
* `09` -> **NAO INICIO DE ATIVIDADE**
* `10` -> **EXTINCAO PELO ENCERRAMENTO DA LIQUIDACAO JUDICIAL**
* `11` -> **ANULACAO POR MULTICIPLIDADE**
* `12` -> **ANULACAO ONLINE DE OFICIO**
* `13` -> **OMISSA CONTUMAZ**
* `14` -> **OMISSA NAO LOCALIZADA**
* `15` -> **INEXISTENCIA DE FATO**
* `16` -> **ANULACAO POR VICIOS**
* `17` -> **BAIXA INICIADA EM ANALISE**
* `18` -> **INTERRUPCAO TEMPORARIA DAS ATIVIDADES**
* `21` -> **PEDIDO DE BAIXA INDEFERIDA**
* `24` -> **POR EMISSAO CERTIDAO NEGATIVA**
* `28` -> **TRANSFERENCIA FILIAL CONDICAO MATRIZ**
* `31` -> **EXTINCAO-UNIFICACAO DA FILIAL**
* `33` -> **TRANSFERENCIA DO ORGAO LOCAL A CONDICAO DE FILIAL DO ORGAO REGIONAL**
* `34` -> **ANULACAO DE INSCRICAO INDEVIDA**
* `35` -> **EMPRESA ESTRANGEIRA AGUARDANDO DOCUMENTACAO**
* `36` -> **PRATICA IRREGULAR DE OPERACAO DE COMERCIO EXTERIOR**
* `37` -> **BAIXA DE PRODUTOR RURAL**
* `38` -> **BAIXA DEFERIDA PELA RFB AGUARDANDO ANALISE DO CONVENENTE**
* `39` -> **BAIXA DEFERIDA PELA RFB E INDEFERIDA PELO CONVENENTE**
* `40` -> **BAIXA INDEFERIDA PELA RFB E AGUARDANDO ANALISE DO CONVENENTE**
* `41` -> **BAIXA INDEFERIDA PELA RFB E DEFERIDA PELO CONVENENTE**
* `42` -> **BAIXA DEFERIDA PELA RFB E SEFIN, AGUARDANDO ANALISE SEFAZ**
* `43` -> **BAIXA DEFERIDA PELA RFB, AGUARDANDO ANALISE DA SEFAZ E INDEFERIDA PELA SEFIN**
* `44` -> **BAIXA DEFERIDA PELA RFB E SEFAZ, AGUARDANDO ANALISE SEFIN**
* `45` -> **BAIXA DEFERIDA PELA RFB, AGUARDANDO ANALISE DA SEFIN E INDEFERIDA PELA SEFAZ**
* `46` -> **BAIXA DEFERIDA PELA RFB E SEFAZ E INDEFERIDA PELA SEFIN**
* `47` -> **BAIXA DEFERIDA PELA RFB E SEFIN E INDEFERIDA PELA SEFAZ**
* `48` -> **BAIXA INDEFERIDA PELA RFB, AGARDANDO ANALISE SEFAZ E DEFERIDA PELA SEFIN**
* `49` -> **BAIXA INDEFERIDA PELA RFB, AGUARDANDO ANALISE DA SEFAZ E INDEFERIDA PELA SEFIN**
* `50` -> **BAIXA INDEFERIDA PELA RFB, DEFERIDA PELA SEFAZ E AGUARDANDO ANALISE DA SEFIN**
* `51` -> **BAIXA INDEFERIDA PELA RFB E SEFAZ, AGUARDANDO ANALISE DA SEFIN**
* `52` -> **BAIXA INDEFERIDA PELA RFB, DEFERIDA PELA SEFAZ E INDEFERIDA PELA SEFIN**
* `53` -> **BAIXA INDEFERIDA PELA RFB E SEFAZ E DEFERIDA PELA SEFIN**
* `54` -> **EXTINCAO - TRATAMENTO DIFERENCIADO DADO AS ME E EPP (LEI COMPLEMENTAR NUMERO 123/2006)**
* `55` -> **DEFERIDO PELO CONVENENTE, AGUARDANDO ANALISE DA RFB**
* `60` -> **ARTIGO 30, VI, DA IN 748/2007**
* `61` -> **INDICIO INTERPOS. FRAUDULENTA**
* `62` -> **FALTA DE PLURALIDADE DE SOCIOS**
* `63` -> **OMISSAO DE DECLARACOES**
* `64` -> **LOCALIZACAO DESCONHECIDA**
* `66` -> **INAPTIDAO**
* `67` -> **REGISTRO CANCELADO**
* `70` -> **ANULACAO POR NAO CONFIRMADO ATO DE REGISTRO DO MEI NA JUNTA COMERCIAL**
* `71` -> **INAPTIDAO (LEI 11.941/2009 ART.54)**
* `72` -> **DETERMINACAO JUDICIAL**
* `73` -> **COOMISSAO CONTUMAZ**
* `74` -> **INCONSISTENCIA CADASTRAL**
* `75` -> **OBITO DO MEI - TITULAR FALECIDO**
* `80` -> **BAIXA REGISTRADA NA JUNTA, INDEFERIDA NA RFB**
* `81` -> **SOLICITACAO DA ADMINISTRACAO TRIBUTARIA ESTADUAL/MUNICIPAL**
* `82` -> **SUSPENSO PERANTE A COMISSAO DE VALORES MOBILIARIOS - CVM**
* `93` -> **CNPJ - TITULAR BAIXADO**

4. **Fallback:**

   * Se o código não estiver listado acima ⇒ `DSC_MOTIVO_SITUCADA_CNPJ = 'NAO INFORMADO'`.

> Observação: a Regra F só deve ser usada se **não houver tabela Oracle** (ou se o lookup falhar). Se houver tabela, usar o lookup como fonte primária e manter a Regra F como contingência.

## Regra G - Seleção e preenchimento de dados do contador (privilegiar CNPJ)

**Objetivo:** preencher os campos de contador na `ODS_BAIXAPOROF_CADASTRO`, escolhendo **um único contador** por estabelecimento, **privilegiando contadorPJ (CNPJ)** quando existir, e obtendo o nome/razão social do contador via consulta no **b-cadastros**.

### G1) Seleção do documento do contador (prioridade PJ)

1. Considere os campos do JSON do estabelecimento (CouchDB b-cadastros):

   * `contadorPJ` (CNPJ do contador)
   * `contadorPF` (CPF do contador)

2. Defina `DOC_CONTADOR` pela seguinte prioridade:

   * Se `contadorPJ` estiver **preenchido** (não NULL, não vazio, após `trim`) -> `DOC_CONTADOR = contadorPJ`
   * Senão, se `contadorPF` estiver **preenchido** -> `DOC_CONTADOR = contadorPF`
   * Senão -> `DOC_CONTADOR = NULL`

### G2) Preenchimento de `NUM_DOC_CONTADOR` e `COD_TIPDOC_CONTADOR`

* Se `DOC_CONTADOR` for NULL:

  * `NUM_DOC_CONTADOR = NULL`
  * `COD_TIPDOC_CONTADOR = NULL`

* Se `DOC_CONTADOR` for preenchido:

  * `NUM_DOC_CONTADOR = DOC_CONTADOR` (armazenar como string, preservando zeros à esquerda)
  * `COD_TIPDOC_CONTADOR`:

    * Se `DOC_CONTADOR` tiver **14 dígitos** -> `'CNPJ'` (ou o código interno equivalente)
    * Se `DOC_CONTADOR` tiver **11 dígitos** -> `'CPF'` (ou o código interno equivalente)
    * Caso contrário -> NULL (ou tratar como inválido conforme regra do ETL)

> Observação: se vier com pontuação, remover máscara antes de contar dígitos.

### G3) Preenchimento de `NOM_RAZAO_SOCIAL_CONT` (consulta obrigatória ao b-cadastros)

Para preencher `NOM_RAZAO_SOCIAL_CONT`, **é necessário consultar o b-cadastros** usando o documento selecionado em `DOC_CONTADOR`:

* Se `COD_TIPDOC_CONTADOR = 'CNPJ'`:

  * Consultar o cadastro do **RAIZ do CNPJ do contador** no b-cadastros (endpoint/base correspondente) e obter:

    * `nomeEmpresarial` 
  * Preencher `NOM_RAZAO_SOCIAL_CONT` com o valor retornado.

* Se `COD_TIPDOC_CONTADOR = 'CPF'`:

  * Consultar o cadastro do **CPF do contador** no b-cadastros (endpoint/base correspondente a PF) e obter:

    * `nomeContribuinte` (campo do cadastro PF)
  * Preencher `NOM_RAZAO_SOCIAL_CONT` com o valor retornado.

* Se a consulta não retornar registro ou o nome vier vazio:

  * `NOM_RAZAO_SOCIAL_CONT = NULL` 

### G4) Preenchimento de `IND_ENDERECO_CONT_FORA_AL`

1. Determine a UF do contador conforme o tipo priorizado:

   * Se foi escolhido `contadorPJ` -> usar `ufCrcContadorPJ`
   * Se foi escolhido `contadorPF` -> usar `ufCrcContadorPF`

2. Regra:

   * Se a UF do contador estiver **preenchida** e **diferente de 'AL'** -> `IND_ENDERECO_CONT_FORA_AL = 'S'`
   * Se a UF do contador for `'AL'` -> `IND_ENDERECO_CONT_FORA_AL = 'N'`
   * Se a UF não estiver preenchida -> `IND_ENDERECO_CONT_FORA_AL = NULL`

## Regra H - Cálculo de `QTD_SOCIO_RAIZ` a partir de `socios`

**Objetivo:** preencher `QTD_SOCIO_RAIZ` usando o campo `socios` do JSON consolidado.

1. Se `socios` **não existir**, for **NULL** ou vier **vazio** (`[]`) -> `QTD_SOCIO_RAIZ = 0`.
2. Se `socios` for **array**:

   * `QTD_SOCIO_RAIZ = quantidade de elementos do array`.

**Exemplo:** se `socios` tiver 3 itens -> `QTD_SOCIO_RAIZ = 3`. Consulte raiz 29091109 para ver a estrutura de `socios`.

