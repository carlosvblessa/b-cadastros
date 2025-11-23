#!/usr/bin/env Rscript

suppressMessages({
  for (pkg in c("dotenv", "DBI", "RPostgres", "httr", "jsonlite", "stringr")) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Instale o pacote '", pkg, "' (install.packages('", pkg, "')).")
    }
  }
})

write_log <- function(...) cat(format(Sys.time()), "-", ..., "\n")
`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

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

couch_scheme <- Sys.getenv("COUCHDB_SCHEME", unset = Sys.getenv("COUCH_SCHEME", "http"))
couch_host   <- Sys.getenv("COUCHDB_HOST",   unset = Sys.getenv("COUCH_HOST", "localhost"))
couch_port   <- Sys.getenv("COUCHDB_PORT",   unset = Sys.getenv("COUCH_PORT", "5984"))
couch_user   <- Sys.getenv("COUCHDB_USER",   unset = Sys.getenv("COUCH_USER", ""))
couch_pass   <- Sys.getenv("COUCHDB_PASSWORD", unset = Sys.getenv("COUCH_PASS", ""))
db_sn        <- Sys.getenv("COUCH_DB_SN", unset = "chsn_bcadastros_replica")

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
    write_log("Bulk GET falhou (status ", httr::status_code(resp), "); fallback individual.")
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

calc_sn_flags <- function(periodos) {
  if (is.null(periodos) || length(periodos) == 0) return(list(ativo = "N", dt_ini = NA, dt_fim = NA))
  df <- jsonlite::fromJSON(jsonlite::toJSON(periodos, auto_unbox = TRUE))
  if (is.null(df)) return(list(ativo = "N", dt_ini = NA, dt_fim = NA))
  if (!is.data.frame(df)) df <- as.data.frame(df, stringsAsFactors = FALSE, check.names = FALSE)
  if (nrow(df) == 0 || !"Inicio" %in% names(df)) return(list(ativo = "N", dt_ini = NA, dt_fim = NA))

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
  ultimo <- df[nrow(df), ]
  ativo <- "N"
  if (is.na(ultimo$FimDate) && !ultimo$Cancelado && !ultimo$Anulado) {
    ativo <- "S"
  }
  list(ativo = ativo, dt_ini = ultimo$InicioDate, dt_fim = ultimo$FimDate)
}

to_date <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA)
  out <- try(as.POSIXct(x[[1]], tz = "UTC"), silent = TRUE)
  if (inherits(out, "try-error")) return(NA)
  as.Date(out)
}

raizes <- DBI::dbGetQuery(
  con,
  "
  select distinct num_cnpj_raiz
  from admb_cads.cad_simples_nacional
  where (ind_mei_ativo = 'S' and dat_exclusao_mei is not null)
     or (ind_simples_ativo = 'S' and dat_exclusao_simples is not null)
  "
)$num_cnpj_raiz

if (length(raizes) == 0) {
  write_log("Nenhum registro inconsistentes de Simples/MEI encontrado para reprocessar.")
  quit(save = "no")
}

write_log("CNPJs raiz a reprocessar: ", length(raizes))

couch_batch_size <- suppressWarnings(as.integer(Sys.getenv("COUCH_BATCH_SIZE", "200")))
if (is.na(couch_batch_size) || couch_batch_size <= 0) couch_batch_size <- 200
write_log("COUCH_BATCH_SIZE = ", couch_batch_size)

idx <- 1
while (idx <= length(raizes)) {
  end_idx <- min(idx + couch_batch_size - 1, length(raizes))
  batch <- raizes[idx:end_idx]
  write_log("Reprocessando batch de ", length(batch), " raizes (", idx, "-", end_idx, ")")

  docs <- couch_get_bulk(db_sn, batch)

  DBI::dbWithTransaction(con, {
    for (raiz in batch) {
      doc <- docs[[raiz]]
      if (is.null(doc)) {
        write_log("SN nao encontrado no Couch para raiz ", raiz, "; pulando.")
        next
      }
      sn  <- calc_sn_flags(doc$PeriodoSimples)
      mei <- calc_sn_flags(doc$PeriodoMEI)

      DBI::dbExecute(
        con,
        "
        UPDATE admb_cads.cad_simples_nacional
        SET ind_simples_ativo      = $1,
            ind_mei_ativo          = $2,
            dat_opcao_simples      = $3,
            dat_exclusao_simples   = $4,
            dat_opcao_mei          = $5,
            dat_exclusao_mei       = $6,
            motivo_exclusao_simples= $7,
            motivo_exclusao_mei    = $8,
            situacao_simples       = $9,
            situacao_mei           = $10
        WHERE num_cnpj_raiz = $11
        ",
        params = list(
          sn$ativo,
          mei$ativo,
          sn$dt_ini,
          sn$dt_fim,
          mei$dt_ini,
          mei$dt_fim,
          doc$MotivoExclusaoSimples %||% NA_character_,
          doc$MotivoExclusaoMei %||% NA_character_,
          doc$SituacaoSimples %||% NA_character_,
          doc$SituacaoMei %||% NA_character_,
          raiz
        )
      )
    }
  })

  idx <- end_idx + 1
}

write_log("Reprocessamento de Simples/MEI concluido.")
