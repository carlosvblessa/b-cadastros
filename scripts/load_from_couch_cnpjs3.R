#!/usr/bin/env Rscript

suppressMessages({
  for (pkg in c("dotenv", "DBI", "RPostgres", "httr", "jsonlite", "stringr")) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Instale o pacote '", pkg, "' (install.packages('", pkg, "')).")
    }
  }
})

dotenv::load_dot_env(".env")

required_vars <- c("DB_NAME", "DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD")
missing_vars <- required_vars[Sys.getenv(required_vars, unset = "") == ""]
if (length(missing_vars) > 0) stop("Configure no .env as variaveis: ", paste(missing_vars, collapse = ", "))

port <- suppressWarnings(as.integer(Sys.getenv("DB_PORT")))
if (is.na(port)) stop("DB_PORT precisa ser numerico (valor atual: ", Sys.getenv("DB_PORT"), ").")

con <- DBI::dbConnect(
  drv = RPostgres::Postgres(),
  dbname = Sys.getenv("DB_NAME"),
  host = Sys.getenv("DB_HOST"),
  port = port,
  user = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD")
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

write_log <- function(...) cat(format(Sys.time()), "-", ..., "\n")
`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

commit_size <- suppressWarnings(as.integer(Sys.getenv("BATCH_COMMIT_SIZE", "50")))
if (is.na(commit_size) || commit_size <= 0) {
  commit_size <- 50
}
write_log("BATCH_COMMIT_SIZE = ", commit_size)

couch_batch_size <- suppressWarnings(as.integer(Sys.getenv("COUCH_BATCH_SIZE", "200")))
if (is.na(couch_batch_size) || couch_batch_size <= 0) {
  couch_batch_size <- 200
}
write_log("COUCH_BATCH_SIZE = ", couch_batch_size)

one_chr <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_character_)
  as.character(x[[1]])
}
one_int <- function(x) {
  v <- one_chr(x)
  if (is.na(v)) return(NA_integer_)
  as.integer(v)
}

map_situacao_cadastral <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_character_)

  cod <- as.character(x[[1]])
  cod <- stringr::str_trim(cod)

  if (!nzchar(cod)) return(NA_character_)

  # garante 2 dígitos se vier "1", "2" etc
  if (nchar(cod) == 1) cod <- paste0("0", cod)

  res <- switch(
    cod,
    "01" = "Nula",
    "02" = "Ativa",
    "03" = "Suspensa",
    "04" = "Inapta",
    "05" = "Ativa Nao Regular",
    "08" = "Baixada",
    NA_character_ # qualquer código inesperado fica como NA
  )

  res
}

map_porte_empresa <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_character_)

  cod <- as.character(x[[1]])
  cod <- stringr::str_trim(cod)

  if (!nzchar(cod)) return(NA_character_)

  # garante 2 dígitos se vier "1", "3", "5"
  if (nchar(cod) == 1) cod <- paste0("0", cod)

  res <- switch(
    cod,
    "01" = "Microempresa",
    "03" = "Empresa de Pequeno Porte",
    "05" = "Demais",
    NA_character_  # qualquer outro código vira NA (ajuste se quiser outro comportamento)
  )

  res
}

ensure_fk <- function(table, column, value) {
  if (is.null(value) || length(value) == 0) return(NA_character_)
  v <- one_chr(value)
  if (is.na(v) || !nzchar(v)) return(NA_character_)
  res <- DBI::dbGetQuery(
    con,
    sprintf("select 1 from admb_cads.%s where %s = $1 limit 1", table, column),
    params = list(v)
  )
  if (nrow(res) == 0) {
    write_log("Valor de FK nao encontrado em ", table, ":", v, " (usando NULL).")
    return(NA_character_)
  }
  v
}

dig <- function(x) {
  if (is.null(x) || length(x) == 0) return(character(0))
  x_chr <- as.character(x)
  stringr::str_replace_all(x_chr, "\\D", "")
}
pad <- function(x, n) {
  if (is.null(x) || length(x) == 0) return(NA_character_)
  nums <- dig(x)
  if (length(nums) == 0) return(NA_character_)
  nums[nums == ""] <- NA_character_
  stringr::str_pad(nums, width = n, pad = "0")
}
parse_date_ymd <- function(x) {
  v <- dig(x)
  if (length(v) == 0) return(NA)
  v <- v[1]
  if (is.na(v) || nchar(v) < 8) return(NA)
  as.Date(v, format = "%Y%m%d")
}
parse_date_iso <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA)
  out <- try(as.POSIXct(x[[1]], tz = "UTC"), silent = TRUE)
  if (inherits(out, "try-error")) return(NA)
  as.Date(out)
}
make_phone <- function(ddd, tel) {
  ddd <- dig(ddd); tel <- dig(tel)
  if (!nzchar(tel)) return(NA_character_)
  if (nzchar(ddd)) paste0("(", ddd, ") ", tel) else tel
}

couch_scheme <- Sys.getenv("COUCHDB_SCHEME", unset = Sys.getenv("COUCH_SCHEME", "http"))
couch_host   <- Sys.getenv("COUCHDB_HOST",   unset = Sys.getenv("COUCH_HOST", "localhost"))
couch_port   <- Sys.getenv("COUCHDB_PORT",   unset = Sys.getenv("COUCH_PORT", "5984"))
couch_user   <- Sys.getenv("COUCHDB_USER",   unset = Sys.getenv("COUCH_USER", ""))
couch_pass   <- Sys.getenv("COUCHDB_PASSWORD", unset = Sys.getenv("COUCH_PASS", ""))

db_cnpj   <- Sys.getenv("COUCH_DB_CNPJ",   unset = "chcnpj_bcadastros_replica")
db_cpf    <- Sys.getenv("COUCH_DB_CPF",    unset = "chcpf_bcadastros_replica")
db_sn     <- Sys.getenv("COUCH_DB_SN",     unset = "chsn_bcadastros_replica")

base_url <- sprintf("%s://%s:%s", couch_scheme, couch_host, couch_port)

.couch_cache <- new.env(parent = emptyenv())

couch_get <- function(db, id) {
  key <- paste0(db, ":", id)
  if (exists(key, envir = .couch_cache, inherits = FALSE)) {
    return(get(key, envir = .couch_cache, inherits = FALSE))
  }
  url <- sprintf("%s/%s/%s", base_url, db, id)
  resp <- httr::GET(url, httr::authenticate(couch_user, couch_pass), httr::timeout(30))
  if (httr::status_code(resp) == 200) {
    doc <- jsonlite::fromJSON(httr::content(resp, as = "text", encoding = "UTF-8"))
  } else if (httr::status_code(resp) == 404) {
    doc <- NULL
  } else {
    stop("Falha ao ler ", url, " (status ", httr::status_code(resp), ").")
  }
  assign(key, doc, envir = .couch_cache)
  doc
}

couch_get_bulk <- function(db, ids) {
  ids <- unique(ids[!is.na(ids)])
  if (length(ids) == 0) return(list())

  # aproveita cache para o que já existe
  out <- list()
  missing <- character(0)
  for (id in ids) {
    key <- paste0(db, ":", id)
    if (exists(key, envir = .couch_cache, inherits = FALSE)) {
      out[[id]] <- get(key, envir = .couch_cache, inherits = FALSE)
    } else {
      missing <- c(missing, id)
    }
  }
  if (length(missing) == 0) return(out)

  url <- sprintf("%s/%s/_all_docs?include_docs=true", base_url, db)
  body <- list(keys = missing)

  resp <- httr::POST(
    url,
    body = jsonlite::toJSON(body, auto_unbox = TRUE),
    encode = "json",
    httr::content_type_json(),
    httr::add_headers(Accept = "application/json"),
    httr::authenticate(couch_user, couch_pass),
    httr::timeout(60)
  )

  if (httr::status_code(resp) != 200) {
    write_log("Bulk GET falhou (status ", httr::status_code(resp), "); fazendo fallback por ID.")
    for (id in missing) {
      out[[id]] <- couch_get(db, id)
    }
    return(out)
  }

  cont <- jsonlite::fromJSON(
    httr::content(resp, as = "text", encoding = "UTF-8"),
    simplifyVector = FALSE
  )

  for (row in cont$rows) {
    if (!is.null(row$error) || is.null(row$doc)) {
      assign(paste0(db, ":", row$id), NULL, envir = .couch_cache)
      next
    }
    assign(paste0(db, ":", row$id), row$doc, envir = .couch_cache)
    out[[row$id]] <- row$doc
  }
  out
}

cnpj_query <- Sys.getenv(
  "CNPJ_LIST_QUERY",
  unset = "select
        cad.num_cnpj
from
        admods001.ods_cadastro obc
inner join admcadapi.cadastro cad on
        obc.num_cnpj = cad.num_cnpj
inner join admcadapi.cad_sefaz_pj sfz on
        cad.num_cnpj = sfz.num_cnpj
where
        cad.dt_ultima_atualizacao <= CURRENT_DATE - interval '16 days'
    and
        (
                (
        not (
            (cad.dsc_situacao_cadastral_cnpj = 'ATIVA'
                        and obc.dsc_situacao_cadastral = 'ATIVO')
                        or
            (cad.dsc_situacao_cadastral_cnpj = 'BAIXADO'
                                and obc.dsc_situacao_cadastral = 'BAIXA')
                        or
            (cad.dsc_situacao_cadastral_cnpj = 'INAPTA'
                                and obc.dsc_situacao_cadastral = 'INAPTO')
                        or
            (cad.dsc_situacao_cadastral_cnpj = 'SUSPENSA'
                                and obc.dsc_situacao_cadastral = 'SUSPENSÃO')
                        or
            (cad.dsc_situacao_cadastral_cnpj = 'NULA'
                                and obc.dsc_situacao_cadastral = 'NULA')
                )
        )
        or
                cad.dsc_situacao_cadastral_cnpj != sfz.dsc_situacao_cadastral_cnpj
    )
order by
        cad.num_cnpj asc"
)

# cnpj_query <- Sys.getenv(
#   "CNPJ_LIST_QUERY",
#   unset = "select distinct num_cnpj from admcadapi.qsa limit 100"
# )

# cnpj_query <- Sys.getenv(
#   "CNPJ_LIST_QUERY",
#   unset = "SELECT num_cnpj
# FROM (
#     VALUES
#         ('00002121000172'),
#         ('00005275000118'),
#         ('00028682000140'),
#         ('00059822000229'),
#         ('00063960012298'),
#         ('00063960056074'),
#         ('00063960056317')
# ) AS lista(num_cnpj)"
# )


cnpj_list <- tryCatch(
  DBI::dbGetQuery(con, cnpj_query),
  error = function(e) stop("Erro ao buscar lista de CNPJs: ", e$message)
)
if (nrow(cnpj_list) == 0) stop("Nenhum CNPJ encontrado na consulta: ", cnpj_query)
col_cnpj <- names(cnpj_list)[1]
cnpjs <- pad(cnpj_list[[col_cnpj]], 14)

if (any(is.na(cnpjs))) stop("CNPJs invalidos na lista (nao numericos ou tamanho diferente de 14).")

cpf_exists <- function(cpf) {
  res <- DBI::dbGetQuery(con, "select 1 from admb_cads.cad_cpf where num_cpf = $1 limit 1", params = list(cpf))
  nrow(res) > 0
}

ensure_cpf <- function(cpf_raw) {
  cpf_clean <- pad(cpf_raw, 11)
  if (is.null(cpf_clean) || length(cpf_clean) == 0) return(NA_character_)
  cpf_clean <- cpf_clean[1]
  if (is.na(cpf_clean)) return(NA_character_)
  if (!cpf_exists(cpf_clean)) {
    cpf_doc <- couch_get(db_cpf, cpf_clean)
    if (is.null(cpf_doc)) {
      write_log("CPF nao encontrado no Couch: ", cpf_clean, " (mantendo NULL para evitar erro de FK).")
      return(NA_character_)
    }
    ok <- tryCatch({
      upsert_cpf(cpf_doc)
      TRUE
    }, error = function(e) {
      write_log("Falha ao inserir CPF ", cpf_clean, " -> ", e$message, " (mantendo NULL para evitar erro de FK).")
      FALSE
    })
    if (!ok) return(NA_character_)
  }
  if (cpf_exists(cpf_clean)) cpf_clean else NA_character_
}

estab_exists <- function(cnpj_full) {
  res <- DBI::dbGetQuery(
    con,
    "select 1 from admb_cads.estabelecimento where num_cnpj = $1 limit 1",
    params = list(cnpj_full)
  )
  nrow(res) > 0
}

upsert_secundarias <- function(doc, cnpj_full) {
  secs <- unlist(doc$cnaeSecundarias %||% list(), use.names = FALSE)
  secs <- stringr::str_trim(as.character(secs))
  secs <- secs[nzchar(secs)]
  DBI::dbExecute(con, "DELETE FROM admb_cads.atividades_secundarias WHERE num_cnpj = $1", params = list(cnpj_full))
  if (length(secs) > 0) {
    for (cnae in secs) {
      cnae_val <- ensure_fk("cad_atividades", "num_atv", cnae)
      if (is.na(cnae_val)) next
      DBI::dbExecute(
        con,
        sprintf(
          "INSERT INTO admb_cads.atividades_secundarias (num_cnpj, cnae_secundaria) VALUES (%s, %s)",
          DBI::dbQuoteString(con, cnpj_full),
          DBI::dbQuoteString(con, cnae_val)
        ),
        immediate = TRUE
      )
    }
  }
}

upsert_cpf <- function(doc) {
  if (is.null(doc)) return(invisible(FALSE))
  num_cpf <- pad(doc$cpfId %||% doc$`_id`, 11)
  if (is.na(num_cpf)) return(invisible(FALSE))
  DBI::dbExecute(
    con,
    "
    INSERT INTO admb_cads.cad_cpf (
      num_cpf, nom_pessoa, nom_social, nom_mae,
      dat_nascimento, ano_obito, dat_inscricao, dat_ultima_atualiz,
      cod_sit_cad_pf, cod_sexo, ind_residente_exterior, ind_estrangeiro,
      cod_municipio_dom_rfb, uf_municipio_dom, logradouro, num_logradouro,
      complemento, bairro, cep, cod_municipio_nat_rfb, uf_municipio_nat,
      cod_pais_nacionalidade, cod_pais_residencia, des_email, num_telefone
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
    ON CONFLICT (num_cpf) DO UPDATE SET
      nom_pessoa = EXCLUDED.nom_pessoa,
      nom_social = EXCLUDED.nom_social,
      nom_mae = EXCLUDED.nom_mae,
      dat_nascimento = EXCLUDED.dat_nascimento,
      ano_obito = EXCLUDED.ano_obito,
      dat_inscricao = EXCLUDED.dat_inscricao,
      dat_ultima_atualiz = EXCLUDED.dat_ultima_atualiz,
      cod_sit_cad_pf = EXCLUDED.cod_sit_cad_pf,
      cod_sexo = EXCLUDED.cod_sexo,
      ind_residente_exterior = EXCLUDED.ind_residente_exterior,
      ind_estrangeiro = EXCLUDED.ind_estrangeiro,
      cod_municipio_dom_rfb = EXCLUDED.cod_municipio_dom_rfb,
      uf_municipio_dom = EXCLUDED.uf_municipio_dom,
      logradouro = EXCLUDED.logradouro,
      num_logradouro = EXCLUDED.num_logradouro,
      complemento = EXCLUDED.complemento,
      bairro = EXCLUDED.bairro,
      cep = EXCLUDED.cep,
      cod_municipio_nat_rfb = EXCLUDED.cod_municipio_nat_rfb,
      uf_municipio_nat = EXCLUDED.uf_municipio_nat,
      cod_pais_nacionalidade = EXCLUDED.cod_pais_nacionalidade,
      cod_pais_residencia = EXCLUDED.cod_pais_residencia,
      des_email = EXCLUDED.des_email,
      num_telefone = EXCLUDED.num_telefone
    ",
    params = list(
      num_cpf,
      one_chr(doc$nomeContribuinte) %||% NA_character_,
      one_chr(doc$nomeSocial) %||% NA_character_,
      one_chr(doc$nomeMae) %||% NA_character_,
      parse_date_ymd(doc$dtNasc),
      one_int(dig(doc$anoObito)),
      parse_date_ymd(doc$dtInscricao),
      parse_date_ymd(doc$dtUltAtualiz),
      one_chr(doc$codSitCad) %||% NA_character_,
      one_chr(doc$codSexo) %||% NA_character_,
      one_chr(doc$indResExt) %||% NA_character_,
      one_chr(doc$indEstrangeiro) %||% NA_character_,
      one_chr(doc$codMunDomic) %||% NA_character_,
      one_chr(doc$ufMunDomic) %||% NA_character_,
      one_chr(doc$logradouro) %||% NA_character_,
      one_chr(doc$nroLogradouro) %||% NA_character_,
      one_chr(doc$complemento) %||% NA_character_,
      one_chr(doc$bairro) %||% NA_character_,
      one_chr(doc$cep) %||% NA_character_,
      one_chr(doc$codMunNat) %||% NA_character_,
      one_chr(doc$ufMunNat) %||% NA_character_,
      one_chr(doc$codPaisNacionalidade) %||% NA_character_,
      one_chr(doc$codPaisResidencia) %||% NA_character_,
      one_chr(doc$email) %||% NA_character_,
      one_chr(doc$telefone) %||% NA_character_
    )
  )
  TRUE
}

upsert_cadastro <- function(doc, cpf_responsavel = NA_character_) {
  if (is.null(doc)) return(invisible(FALSE))
  raiz <- pad(doc$cnpj %||% doc$`_id`, 8)
  if (is.na(raiz)) return(invisible(FALSE))
  capital <- doc$capitalSocial %||% NA_character_
  capital_num <- suppressWarnings(as.numeric(capital))
  if (!is.na(capital_num) && capital_num > 0) capital_num <- capital_num / 100 else capital_num <- NA_real_
  cpf_resp_use <- if (!is.na(cpf_responsavel)) cpf_responsavel else NA_character_
  DBI::dbExecute(
    con,
    "
    INSERT INTO admb_cads.cadastro (
      num_cnpj_raiz, nome_empresarial, natureza_juridica,
      porte_empresa, capital_social, cpf_responsavel,
      qualificacao_resp, data_inclusao_resp
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (num_cnpj_raiz) DO UPDATE SET
      nome_empresarial = EXCLUDED.nome_empresarial,
      natureza_juridica = EXCLUDED.natureza_juridica,
      porte_empresa = EXCLUDED.porte_empresa,
      capital_social = EXCLUDED.capital_social,
      cpf_responsavel = EXCLUDED.cpf_responsavel,
      qualificacao_resp = EXCLUDED.qualificacao_resp,
      data_inclusao_resp = EXCLUDED.data_inclusao_resp
    ",
    params = list(
      raiz,
      one_chr(doc$nomeEmpresarial) %||% NA_character_,
      one_chr(doc$naturezaJuridica) %||% NA_character_,
      map_porte_empresa(doc$porteEmpresa),
      capital_num,
      cpf_resp_use,
      one_chr(doc$qualificacaoResponsavel) %||% NA_character_,
      parse_date_ymd(one_chr(doc$dataInclusaoResponsavel))
    )
  )
  TRUE
}

upsert_estabelecimento <- function(doc, cpf_contador_pf = NA_character_, contador_pj_val = NA_character_) {
  if (is.null(doc)) return(invisible(FALSE))
  cnpj_full <- pad(one_chr(doc$cnpj %||% doc$`_id`), 14)
  if (is.na(cnpj_full)) return(invisible(FALSE))
  raiz <- substr(cnpj_full, 1, 8)
  tel1 <- make_phone(one_chr(doc$dddTelefone1), one_chr(doc$telefone1))
  tel2 <- make_phone(one_chr(doc$dddTelefone2), one_chr(doc$telefone2))
  sec_vec <- unlist(doc$cnaeSecundarias %||% list(), use.names = FALSE)
  sec_vec <- as.character(sec_vec)
  sec_vec <- sec_vec[!is.na(sec_vec) & nzchar(sec_vec)]
  sec_param <- if (length(sec_vec) == 0) "{}" else paste0("{", paste(sec_vec, collapse = ","), "}")
  motivo_val <- ensure_fk("cad_motivo_situcada", "cod_motivo_situcada", doc$motivoSituacao)
  mun_val <- ensure_fk("cad_municipio", "cod_municipio_rfb", doc$codigoMunicipio)
  cnae_princ_val <- ensure_fk("cad_atividades", "num_atv", doc$cnaeFiscal)
  DBI::dbExecute(
    con,
    "
    INSERT INTO admb_cads.estabelecimento (
      num_cnpj, num_cnpj_raiz, nome_fantasia, indicador_matriz,
      situacao_cadastral, data_situcada, motivo_situcada,
      tipo_logradouro, logradouro, numero, complemento, bairro, cep,
      cod_municipio_rfb, uf, email, telefone1, telefone2,
      contador_pf, contador_pj, uf_crc_contador_pf, uf_crc_contador_pj,
      seq_crc_contador_pf, seq_crc_contador_pj, tipo_crc_contador_pf, tipo_crc_contador_pj,
      data_inicio_atividade, cnae_principal
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)
    ON CONFLICT (num_cnpj) DO UPDATE SET
      num_cnpj_raiz = EXCLUDED.num_cnpj_raiz,
      nome_fantasia = EXCLUDED.nome_fantasia,
      indicador_matriz = EXCLUDED.indicador_matriz,
      situacao_cadastral = EXCLUDED.situacao_cadastral,
      data_situcada = EXCLUDED.data_situcada,
      motivo_situcada = EXCLUDED.motivo_situcada,
      tipo_logradouro = EXCLUDED.tipo_logradouro,
      logradouro = EXCLUDED.logradouro,
      numero = EXCLUDED.numero,
      complemento = EXCLUDED.complemento,
      bairro = EXCLUDED.bairro,
      cep = EXCLUDED.cep,
      cod_municipio_rfb = EXCLUDED.cod_municipio_rfb,
      uf = EXCLUDED.uf,
      email = EXCLUDED.email,
      telefone1 = EXCLUDED.telefone1,
      telefone2 = EXCLUDED.telefone2,
      contador_pf = EXCLUDED.contador_pf,
      contador_pj = EXCLUDED.contador_pj,
      uf_crc_contador_pf = EXCLUDED.uf_crc_contador_pf,
      uf_crc_contador_pj = EXCLUDED.uf_crc_contador_pj,
      seq_crc_contador_pf = EXCLUDED.seq_crc_contador_pf,
      seq_crc_contador_pj = EXCLUDED.seq_crc_contador_pj,
      tipo_crc_contador_pf = EXCLUDED.tipo_crc_contador_pf,
      tipo_crc_contador_pj = EXCLUDED.tipo_crc_contador_pj,
      data_inicio_atividade = EXCLUDED.data_inicio_atividade,
      cnae_principal = EXCLUDED.cnae_principal
    ",
    params = list(
      cnpj_full,
      raiz,
      one_chr(doc$nomeFantasia) %||% NA_character_,
      one_chr(doc$indicadorMatriz) %||% NA_character_,
      map_situacao_cadastral(doc$situacaoCadastral),
      parse_date_ymd(one_chr(doc$dataSituacaoCadastral)),
      motivo_val,
      one_chr(doc$tipoLogradouro) %||% NA_character_,
      one_chr(doc$logradouro) %||% NA_character_,
      one_chr(doc$numero) %||% NA_character_,
      one_chr(doc$complemento) %||% NA_character_,
      one_chr(doc$bairro) %||% NA_character_,
      one_chr(doc$cep) %||% NA_character_,
      mun_val,
      one_chr(doc$uf) %||% NA_character_,
      one_chr(doc$email) %||% NA_character_,
      tel1,
      tel2,
      if (!is.na(cpf_contador_pf)) cpf_contador_pf else NA_character_,
      if (!is.na(contador_pj_val)) contador_pj_val else NA_character_,
      one_chr(doc$ufCrcContadorPF) %||% NA_character_,
      one_chr(doc$ufCrcContadorPJ) %||% NA_character_,
      one_chr(doc$sequencialCrcContadorPF) %||% NA_character_,
      one_chr(doc$sequencialCrcContadorPJ) %||% NA_character_,
      one_chr(doc$tipoCrcContadorPF) %||% NA_character_,
      one_chr(doc$tipoCrcContadorPJ) %||% NA_character_,
      parse_date_ymd(one_chr(doc$dataInicioAtividade)),
      cnae_princ_val
    )
  )
  TRUE
}

calc_sn_flags <- function(periodos) {
  if (is.null(periodos) || length(periodos) == 0) return(list(ativo = "N", dt_ini = NA, dt_fim = NA))
  df <- jsonlite::fromJSON(jsonlite::toJSON(periodos, auto_unbox = TRUE))
  if (is.null(df)) return(list(ativo = "N", dt_ini = NA, dt_fim = NA))
  if (!is.data.frame(df)) df <- as.data.frame(df, stringsAsFactors = FALSE, check.names = FALSE)
  if (nrow(df) == 0 || !"Inicio" %in% names(df)) return(list(ativo = "N", dt_ini = NA, dt_fim = NA))

  # Inicio pode vir como vetor simples ou list-column
  inicio_vec <- vapply(seq_len(nrow(df)), function(i) {
    if (!("Inicio" %in% names(df)) || length(df$Inicio) < i) return(NA_character_)
    val <- df$Inicio[[i]]
    if (is.null(val) || length(val) == 0) return(NA_character_)
    substr(as.character(val[[1]]), 1, 10)
  }, character(1))
  df$InicioDate <- as.Date(inicio_vec)

  fim_vec <- vapply(seq_len(nrow(df)), function(i) {
    if (!"Fim" %in% names(df) || length(df$Fim) < i) return(NA_character_)
    val <- df$Fim[[i]]
    if (is.null(val) || length(val) == 0) return(NA_character_)
    substr(as.character(val[[1]]), 1, 10)
  }, character(1))
  df$FimDate <- as.Date(fim_vec)

  cancel_vec <- vapply(seq_len(nrow(df)), function(i) {
    if (!"Cancelado" %in% names(df) || length(df$Cancelado) < i) return(FALSE)
    val <- df$Cancelado[[i]]
    if (is.null(val) || length(val) == 0) return(FALSE)
    as.logical(val[[1]])
  }, logical(1))
  df$Cancelado <- cancel_vec

  anulado_vec <- vapply(seq_len(nrow(df)), function(i) {
    if (!"Anulado" %in% names(df) || length(df$Anulado) < i) return(FALSE)
    val <- df$Anulado[[i]]
    if (is.null(val) || length(val) == 0) return(FALSE)
    as.logical(val[[1]])
  }, logical(1))
  df$Anulado <- anulado_vec

  df <- df[order(df$InicioDate), , drop = FALSE]
  ativo <- "N"
  # consideramos apenas o ultimo periodo (situacao atual)
  ultimo <- df[nrow(df), ]
  dt_ini <- ultimo$InicioDate
  dt_fim <- ultimo$FimDate
  if (is.na(dt_fim) && !ultimo$Cancelado && !ultimo$Anulado) {
    ativo <- "S"
  }
  list(ativo = ativo, dt_ini = dt_ini, dt_fim = dt_fim)
}

upsert_simples <- function(doc, raiz) {
  if (is.null(doc)) return(invisible(FALSE))
  sn <- calc_sn_flags(doc$PeriodoSimples)
  mei <- calc_sn_flags(doc$PeriodoMEI)
  DBI::dbExecute(
    con,
    "
    INSERT INTO admb_cads.cad_simples_nacional (
      num_cnpj_raiz, ind_simples_ativo, ind_mei_ativo,
      dat_opcao_simples, dat_exclusao_simples,
      dat_opcao_mei, dat_exclusao_mei,
      motivo_exclusao_simples, motivo_exclusao_mei,
      situacao_simples, situacao_mei
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    ON CONFLICT (num_cnpj_raiz) DO UPDATE SET
      ind_simples_ativo = EXCLUDED.ind_simples_ativo,
      ind_mei_ativo = EXCLUDED.ind_mei_ativo,
      dat_opcao_simples = EXCLUDED.dat_opcao_simples,
      dat_exclusao_simples = EXCLUDED.dat_exclusao_simples,
      dat_opcao_mei = EXCLUDED.dat_opcao_mei,
      dat_exclusao_mei = EXCLUDED.dat_exclusao_mei,
      motivo_exclusao_simples = EXCLUDED.motivo_exclusao_simples,
      motivo_exclusao_mei = EXCLUDED.motivo_exclusao_mei,
      situacao_simples = EXCLUDED.situacao_simples,
      situacao_mei = EXCLUDED.situacao_mei
    ",
    params = list(
      raiz,
      sn$ativo,
      mei$ativo,
      sn$dt_ini,
      sn$dt_fim,
      mei$dt_ini,
      mei$dt_fim,
      doc$MotivoExclusaoSimples %||% NA_character_,
      doc$MotivoExclusaoMei %||% NA_character_,
      doc$SituacaoSimples %||% NA_character_,
      doc$SituacaoMei %||% NA_character_
    )
  )
  TRUE
}

processar_socios <- function(cnpj_full, socios) {
  socios <- socios %||% list()
  if (length(socios) == 0) return(list(novos_cnpjs = character(0), rows = list()))
  conteudo <- jsonlite::fromJSON(jsonlite::toJSON(socios, auto_unbox = TRUE))
  if (nrow(conteudo) == 0) return(list(novos_cnpjs = character(0), rows = list()))
  drops <- c("cpfCnpj", "cnpjCpf", "cpfCnpjSocio", "cnpjCpfSocio", "cpfSocio")
  novos_cnpjs <- character(0)
  rows <- list()
  for (i in seq_len(nrow(conteudo))) {
    socio_row <- conteudo[i, , drop = FALSE]
    socio_row <- lapply(socio_row, one_chr)
    socio_id_raw <- NA_character_
    for (k in drops) {
      if (k %in% names(socio_row) && nzchar(socio_row[[k]])) {
        socio_id_raw <- socio_row[[k]]
        break
      }
    }
    socio_id_full <- dig(socio_id_raw)
    socio_id_full <- if (length(socio_id_full) == 0) NA_character_ else socio_id_full[1]
    socio_cpf <- NA_character_
    socio_cnpj <- NA_character_
    tipo_socio <- NA_character_
    tipo_flag <- socio_row$tipo %||% NA_character_

    if (!is.na(tipo_flag)) {
      tipo_flag <- as.character(tipo_flag)
      if (tipo_flag == "2") { # Pessoa Fisica
        if (!is.na(socio_id_full) && nchar(socio_id_full) == 14 && grepl("^000", socio_id_full)) {
          socio_cpf <- substr(socio_id_full, 4, 14)
        } else {
          socio_cpf <- socio_id_full
        }
        tipo_socio <- "F"
      } else if (tipo_flag == "1") { # Pessoa Juridica
        socio_cnpj <- socio_id_full
        tipo_socio <- "J"
      } else if (tipo_flag == "3") { # Estrangeiro
        socio_cnpj <- socio_id_full
        tipo_socio <- "E"
      }
    }

    if (is.na(tipo_socio)) {
      if (!is.na(socio_id_full)) {
        if (nchar(socio_id_full) == 11) {
          socio_cpf <- socio_id_full
          tipo_socio <- "F"
        } else if (nchar(socio_id_full) == 14 && grepl("^000", socio_id_full)) {
          socio_cpf <- substr(socio_id_full, 4, 14)
          tipo_socio <- "F"
        } else if (nchar(socio_id_full) == 14) {
          socio_cnpj <- socio_id_full
          tipo_socio <- "J"
        }
      }
    }
    qual_socio <- socio_row$qualificacaoSocio %||% socio_row$qualificacao %||% NA_character_
    cpf_rep_raw <- socio_row$cpfRepresentanteLegal %||% socio_row$cpfRepresentante
    cpf_rep <- ensure_cpf(cpf_rep_raw)
    qual_rep <- socio_row$qualificacaoRepresentanteLegal %||% socio_row$qualificacaoRepLegal %||% NA_character_
    data_ent <- parse_date_ymd(socio_row$dataEntradaSociedade %||% socio_row$dataEntrada)

    rows[[length(rows) + 1]] <- list(
      num_cnpj = cnpj_full,
      cpf_socio = socio_cpf,
      cnpj_socio = socio_cnpj,
      tipo_socio = tipo_socio,
      qual_socio = qual_socio,
      cpf_rep = cpf_rep,
      qual_rep = qual_rep,
      data_ent = data_ent
    )

    if (!is.na(socio_cnpj)) {
      novos_cnpjs <- c(novos_cnpjs, pad(socio_cnpj, 14))
    }
  }
  list(novos_cnpjs = unique(novos_cnpjs), rows = rows)
}

# Processamento em BFS com commits por tamanho de lote
pending_qsa <- list()
queue <- unique(cnpjs)
processed <- character(0)
pending_contador_pj <- list()
batch_count <- 0
DBI::dbBegin(con)

resolve_contador_pj <- function() {
  if (length(pending_contador_pj) == 0) return()
  remaining <- list()
  for (lnk in pending_contador_pj) {
    if (estab_exists(lnk$contador_pj) && estab_exists(lnk$num_cnpj)) {
      sql_upd <- sprintf(
        "UPDATE admb_cads.estabelecimento SET contador_pj = %s WHERE num_cnpj = %s",
        DBI::dbQuoteString(con, lnk$contador_pj),
        DBI::dbQuoteString(con, lnk$num_cnpj)
      )
      DBI::dbExecute(con, sql_upd, immediate = TRUE)
      write_log("Atualizado contador PJ ", lnk$contador_pj, " para estab ", lnk$num_cnpj)
    } else {
      remaining[[length(remaining) + 1]] <- lnk
    }
  }
  pending_contador_pj <<- remaining
}

flush_batch <- function() {
  # tenta resolver contadores pendentes que ja existem
  resolve_contador_pj()

  # insere QSA pendente (somente se as dependencias existem)
  if (length(pending_qsa) > 0) {
    for (row in pending_qsa) {
      socio_cpf_val <- if (!is.na(row$cpf_socio)) ensure_cpf(row$cpf_socio) else NA_character_
      socio_cnpj_val <- if (!is.na(row$cnpj_socio)) pad(row$cnpj_socio, 14) else NA_character_

      if (!is.na(socio_cnpj_val) && !estab_exists(socio_cnpj_val)) {
        # ainda nao existe o PJ do socio; deixa para a proxima
        next
      }

      q <- function(val) if (is.na(val) || length(val) == 0) "NULL" else DBI::dbQuoteString(con, val)
      sql_qsa <- sprintf(
        "
        INSERT INTO admb_cads.qsa
          (num_cnpj, cpf_socio, cnpj_socio, qualificacao_socio, tipo_socio,
           cpf_representante, qualificacao_rep, data_entrada)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
        ",
        q(row$num_cnpj),
        q(socio_cpf_val),
        q(socio_cnpj_val),
        q(row$qual_socio),
        q(row$tipo_socio),
        q(ensure_cpf(row$cpf_rep)),
        q(row$qual_rep),
        if (is.na(row$data_ent) || length(row$data_ent) == 0) "NULL" else DBI::dbQuoteString(con, as.character(row$data_ent))
      )
      DBI::dbExecute(con, sql_qsa, immediate = TRUE)
    }
    # remove os que foram inseridos (dependencia resolvida)
    pending_qsa <<- Filter(function(r) {
      socio_cnpj_val <- if (!is.na(r$cnpj_socio)) pad(r$cnpj_socio, 14) else NA_character_
      !is.na(socio_cnpj_val) && !estab_exists(socio_cnpj_val)
    }, pending_qsa)
  }

  DBI::dbCommit(con)
  DBI::dbBegin(con)
  batch_count <<- 0
  write_log("Commit aplicado; pendentes contador_pj=", length(pending_contador_pj), ", pendentes QSA=", length(pending_qsa))
}

while (length(queue) > 0) {
  idxs  <- seq_len(min(couch_batch_size, length(queue)))
  batch <- queue[idxs]
  queue <- queue[-idxs]

  batch <- setdiff(batch, processed)
  if (length(batch) == 0) next

  processed <- c(processed, batch)
  write_log("Processando batch de ", length(batch), " CNPJs")

  raizes <- substr(batch, 1, 8)
  raizes_unicas <- unique(raizes)

  cad_docs <- couch_get_bulk(db_cnpj, raizes_unicas)
  est_docs <- couch_get_bulk(db_cnpj, batch)
  sn_docs  <- couch_get_bulk(db_sn, raizes_unicas)

  for (cnpj in batch) {
    write_log("Processando CNPJ", cnpj)
    raiz <- substr(cnpj, 1, 8)

    cad_doc <- cad_docs[[raiz]]
    est_doc <- est_docs[[cnpj]]
    sn_doc  <- sn_docs[[raiz]]

    # garanta CPF do responsavel e contador PF antes de inserir FK
    cpf_resp_safe <- NA_character_
    cpf_contador_safe <- NA_character_
    contador_pj_safe <- NA_character_
    if (!is.null(cad_doc) && !is.null(cad_doc$cpfResponsavel)) {
      cpf_resp_safe <- ensure_cpf(cad_doc$cpfResponsavel)
      if (is.na(cpf_resp_safe)) write_log("CPF do responsavel nao inserido (nao encontrado ou falha de FK); FK no cadastro ficara NULL.")
    }
    if (!is.null(est_doc) && !is.null(est_doc$contadorPF)) {
      cpf_contador_safe <- ensure_cpf(est_doc$contadorPF)
      if (is.na(cpf_contador_safe)) write_log("Contador PF nao encontrado/inserido para CNPJ ", cnpj, "; FK ficara NULL.")
    }
    if (!is.null(est_doc) && !is.null(est_doc$contadorPJ)) {
      contador_pj_safe <- pad(one_chr(est_doc$contadorPJ), 14)
      if (!is.na(contador_pj_safe) && contador_pj_safe != cnpj) {
        if (!estab_exists(contador_pj_safe) && !(contador_pj_safe %in% processed)) {
          if (!(contador_pj_safe %in% queue)) {
            write_log("Enfileirando contador PJ ", contador_pj_safe, " antes de carregar CNPJ ", cnpj)
            queue <- c(queue, contador_pj_safe)
          } else {
            write_log("Contador PJ ", contador_pj_safe, " ja enfileirado.")
          }
          pending_contador_pj[[length(pending_contador_pj) + 1]] <- list(num_cnpj = cnpj, contador_pj = contador_pj_safe)
          contador_pj_safe <- NA_character_
        }
      }
    }

    # 1) cadastro primeiro (FK de estabelecimento depende dele)
    if (!is.null(cad_doc)) {
      upsert_cadastro(cad_doc, cpf_responsavel = cpf_resp_safe)
    } else {
      write_log("Cadastro nao encontrado no Couch para raiz ", raiz, "; pulando estabelecimento/QSA para evitar erro de FK.")
    }

    # 2) estabelecimento (so se cadastro existe)
    estab_ok <- FALSE
    if (!is.null(est_doc)) {
      if (!is.null(cad_doc)) {
        upsert_estabelecimento(est_doc, cpf_contador_pf = cpf_contador_safe, contador_pj_val = contador_pj_safe)
        upsert_secundarias(est_doc, cnpj_full = cnpj)
        estab_ok <- TRUE
      } else {
        write_log("Estabelecimento nao carregado porque o cadastro da raiz ", raiz, " nao existe na fonte.")
      }
    } else {
      write_log("Estabelecimento nao encontrado no Couch para CNPJ ", cnpj)
    }

    # 3) QSA so se cadastro/estab existem; enfileira socios PJ encontrados
    if (!is.null(cad_doc) && estab_ok) {
      res_soc <- processar_socios(cnpj_full = cnpj, socios = cad_doc$socios)
      novos_cnpjs <- res_soc$novos_cnpjs
      pending_qsa <- c(pending_qsa, res_soc$rows)
      novos_cnpjs <- setdiff(unique(novos_cnpjs), c(processed, queue))
      if (length(novos_cnpjs) > 0) {
        write_log("Enfileirando socios PJ para carga: ", paste(novos_cnpjs, collapse = ", "))
        queue <- c(queue, novos_cnpjs)
      }
    }

    # 4) Simples Nacional (pode existir mesmo sem estab)
    if (!is.null(sn_doc)) {
      upsert_simples(sn_doc, raiz)
    } else {
      write_log("Documento de Simples/MEI nao encontrado para raiz ", raiz, " (empresa pode nunca ter optado).")
    }

    # tenta resolver contadores pendentes logo apos processar este CNPJ
    resolve_contador_pj()

    batch_count <- batch_count + 1
    if (batch_count >= commit_size) {
      flush_batch()
    }
  }
}

# flush final para pendencias restantes
if (batch_count > 0 || length(pending_contador_pj) > 0 || length(pending_qsa) > 0) {
  flush_batch()
}

write_log("Carga via CouchDB finalizada. Processados: ", length(processed), " CNPJs (incluindo socios PJ enfileirados). QSA inserido apos dependencia.")
