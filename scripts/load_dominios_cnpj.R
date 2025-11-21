#!/usr/bin/env Rscript

suppressMessages({
  if (!requireNamespace("dotenv", quietly = TRUE)) {
    stop("Instale o pacote 'dotenv' (install.packages('dotenv')).")
  }
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Instale o pacote 'DBI' (install.packages('DBI')).")
  }
  if (!requireNamespace("RPostgres", quietly = TRUE)) {
    stop("Instale o pacote 'RPostgres' (install.packages('RPostgres')).")
  }
  if (!requireNamespace("readr", quietly = TRUE)) {
    stop("Instale o pacote 'readr' (install.packages('readr')).")
  }
  if (!requireNamespace("httr", quietly = TRUE)) {
    stop("Instale o pacote 'httr' (install.packages('httr')).")
  }
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Instale o pacote 'dplyr' (install.packages('dplyr')).")
  }
  if (!requireNamespace("glue", quietly = TRUE)) {
    stop("Instale o pacote 'glue' (install.packages('glue')).")
  }
  if (!requireNamespace("xml2", quietly = TRUE)) {
    stop("Instale o pacote 'xml2' (install.packages('xml2')).")
  }
})

dotenv::load_dot_env(".env")

required_vars <- c("DB_NAME", "DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD")
missing_vars <- required_vars[Sys.getenv(required_vars, unset = "") == ""]

if (length(missing_vars) > 0) {
  stop("Configure no .env as variaveis: ", paste(missing_vars, collapse = ", "))
}

port <- suppressWarnings(as.integer(Sys.getenv("DB_PORT")))
if (is.na(port)) {
  stop("DB_PORT precisa ser numerico (valor atual: ", Sys.getenv("DB_PORT"), ").")
}

con <- DBI::dbConnect(
  drv = RPostgres::Postgres(),
  dbname = Sys.getenv("DB_NAME"),
  host = Sys.getenv("DB_HOST"),
  port = port,
  user = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD")
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

schema_name <- "admb_cads"

# -----------------------------------------------------------
# Helpers
# -----------------------------------------------------------
write_log <- function(...) cat(format(Sys.time()), "-", ..., "\n")

CNPJ_BASE_URL <- "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"
DEFAULT_PASTA <- "2025-11"  # fallback se nao conseguir descobrir
parse_http_date_safe <- function(hdr) {
  if (is.null(hdr) || length(hdr) == 0) return(NA_real_)
  ts <- try(httr::parse_http_date(hdr), silent = TRUE)
  if (inherits(ts, "try-error") || length(ts) == 0) return(NA_real_)
  ts
}

table_exists <- function(con, schema, table) {
  res <- DBI::dbGetQuery(
    con,
    "
    select 1
    from information_schema.tables
    where table_schema = $1
      and table_name   = $2
    limit 1
    ",
    params = list(schema, table)
  )
  nrow(res) == 1
}

required_tables <- c(
  "cad_atividades",
  "cad_natureza_juridica",
  "cad_tipo_socio",
  "cad_motivo_situcada",
  "cad_pais",
  "cad_municipio",
  "ctrl_carga_dominios_cnpj"
)

missing <- required_tables[!vapply(required_tables, table_exists, logical(1), con = con, schema = schema_name)]
if (length(missing) > 0) {
  stop(
    "Tabelas ausentes no schema ", schema_name, ": ",
    paste(missing, collapse = ", "),
    ". Rode 'scripts/setup_postgres.R' antes de carregar os dominios."
  )
}

detectar_pasta_mais_recente <- function(base_url = CNPJ_BASE_URL, fallback = DEFAULT_PASTA) {
  resp <- try(httr::GET(base_url, httr::timeout(20)), silent = TRUE)
  if (inherits(resp, "try-error")) {
    write_log("Aviso: nao consegui acessar", base_url, "- usando pasta fallback", fallback)
    return(fallback)
  }
  if (httr::status_code(resp) != 200) {
    write_log("Aviso: status", httr::status_code(resp), "ao consultar", base_url, "- usando pasta fallback", fallback)
    return(fallback)
  }
  txt <- httr::content(resp, as = "text", encoding = "UTF-8")
  matches <- unique(gsub("/", "", regmatches(txt, gregexpr("\\d{4}-\\d{2}/", txt, perl = TRUE))[[1]]))
  if (length(matches) == 0) {
    write_log("Aviso: nenhuma pasta encontrada em", base_url, "- usando", fallback)
    return(fallback)
  }
  latest <- sort(matches, decreasing = TRUE)[1]
  write_log("Pasta mais recente detectada em", base_url, "->", latest)
  latest
}

ultima_carga_ok <- function(con, pasta, nome_zip) {
  res <- DBI::dbGetQuery(
    con,
    "
    SELECT data_carga
    FROM admb_cads.ctrl_carga_dominios_cnpj
    WHERE pasta_ref = $1
      AND arquivo   = $2
      AND status    = 'OK'
    ORDER BY data_carga DESC
    LIMIT 1
    ",
    params = list(pasta, nome_zip)
  )
  if (nrow(res) == 0) return(NULL)
  res$data_carga[[1]]
}

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

obter_last_modified <- function(pasta, nome_zip) {
  url_zip <- glue::glue("{CNPJ_BASE_URL}/{pasta}/{nome_zip}")
  resp_zip <- try(httr::HEAD(url_zip, httr::timeout(15)), silent = TRUE)

  if (!inherits(resp_zip, "try-error") && httr::status_code(resp_zip) == 200) {
    ts <- parse_http_date_safe(httr::headers(resp_zip)[["last-modified"]])
    if (!is.na(ts)) return(ts)
  }

  # fallback 1: HEAD na pasta
  url_pasta <- glue::glue("{CNPJ_BASE_URL}/{pasta}/")
  resp_pasta <- try(httr::HEAD(url_pasta, httr::timeout(15)), silent = TRUE)
  if (!inherits(resp_pasta, "try-error") && httr::status_code(resp_pasta) == 200) {
    ts <- parse_http_date_safe(httr::headers(resp_pasta)[["last-modified"]])
    if (!is.na(ts)) return(ts)
  }

  # fallback 2: ler HTML da pasta e pegar um zip (o proprio nome_zip, se listado) e HEAD nele
  resp_list <- try(httr::GET(url_pasta, httr::timeout(20)), silent = TRUE)
  if (!inherits(resp_list, "try-error") && httr::status_code(resp_list) == 200) {
    hrefs <- try(xml2::xml_attr(xml2::xml_find_all(xml2::read_html(httr::content(resp_list, as = "text", encoding = "UTF-8")), ".//a"), "href"), silent = TRUE)
    if (!inherits(hrefs, "try-error")) {
      zips <- grep("\\.zip$", hrefs, value = TRUE, ignore.case = TRUE)
      if (length(zips) > 0) {
        alvo <- if (nome_zip %in% zips) nome_zip else zips[1]
        url_any <- paste0(url_pasta, alvo)
        resp_any <- try(httr::HEAD(url_any, httr::timeout(15)), silent = TRUE)
        if (!inherits(resp_any, "try-error") && httr::status_code(resp_any) == 200) {
          ts <- parse_http_date_safe(httr::headers(resp_any)[["last-modified"]])
          if (!is.na(ts)) return(ts)
        }
      }
    }
  }

  NA_real_
}

registrar_carga_zip <- function(con, pasta, nome_zip, status = "OK", msg_erro = NULL) {
  status <- status %||% "OK"
  msg_erro <- msg_erro %||% NA_character_
  DBI::dbExecute(
    con,
    "
    INSERT INTO admb_cads.ctrl_carga_dominios_cnpj
      (pasta_ref, arquivo, status, msg_erro)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (pasta_ref, arquivo) DO UPDATE
      SET status     = EXCLUDED.status,
          msg_erro   = EXCLUDED.msg_erro,
          data_carga = now()
    ",
    params = list(pasta, nome_zip, status, msg_erro)
  )
}

baixar_ler_zip <- function(pasta_aaaa_mm, nome_zip, delim = ";") {
  url <- glue::glue("{CNPJ_BASE_URL}/{pasta_aaaa_mm}/{nome_zip}")
  write_log("Baixando", nome_zip, "de", url)

  arq_zip <- tempfile(fileext = ".zip")
  resp <- httr::GET(url, httr::write_disk(arq_zip, overwrite = TRUE), httr::timeout(120))

  if (httr::status_code(resp) != 200) {
    stop("Erro ao baixar ", nome_zip, " - HTTP ", httr::status_code(resp))
  }

  arquivos <- utils::unzip(arq_zip, exdir = tempdir())
  if (length(arquivos) < 1) {
    stop("Nenhum arquivo encontrado dentro de ", nome_zip)
  }
  if (length(arquivos) > 1) {
    write_log("Aviso: mais de um arquivo dentro de", nome_zip, "- usando o primeiro.")
  }
  arq_txt <- arquivos[1]

  readr::read_delim(
    arq_txt,
    delim = delim,
    locale = readr::locale(encoding = "Latin1"),
    col_types = readr::cols(.default = readr::col_character()),
    trim_ws = TRUE,
    progress = FALSE
  )
}

processar_zip_para_tabela <- function(con, pasta, cfg) {
  nome_zip <- cfg$zip

  force_reload <- tolower(Sys.getenv("CNPJ_FORCE_RELOAD", unset = "false")) %in% c("1", "true", "yes", "y")
  ultima_ok <- ultima_carga_ok(con, pasta, nome_zip)
  remoto_lastmod <- obter_last_modified(pasta, nome_zip)

  if (!force_reload && !is.null(ultima_ok)) {
    if (!is.na(remoto_lastmod) && remoto_lastmod <= ultima_ok) {
      write_log(nome_zip, "ja carregado para a pasta", pasta, "(ultima carga:", ultima_ok, ", last-mod:", remoto_lastmod, ") - pulando.")
      return(invisible(FALSE))
    }
    if (is.na(remoto_lastmod)) {
      write_log(nome_zip, "ja carregado para a pasta", pasta, "e nao consegui ler Last-Modified remoto - pulando.")
      return(invisible(FALSE))
    }
    write_log(nome_zip, "remoto mais novo que a ultima carga (ultima:", ultima_ok, ", last-mod:", remoto_lastmod, ") - recarregando.")
  } else if (force_reload) {
    write_log("CNPJ_FORCE_RELOAD ativo - recarregando ", nome_zip, " mesmo que ja exista.")
  }

  write_log("Iniciando carga de", nome_zip, "para a pasta", pasta)

  tryCatch({
    df_raw <- baixar_ler_zip(pasta, nome_zip)
    if (ncol(df_raw) < length(cfg$cols)) {
      stop("Arquivo ", nome_zip, " veio com ", ncol(df_raw), " colunas; esperado >= ", length(cfg$cols))
    }
    df <- df_raw[, seq_len(length(cfg$cols))]
    names(df) <- cfg$cols

    df <- dplyr::mutate(df, dplyr::across(dplyr::everything(), ~ trimws(.x)))

    DBI::dbWithTransaction(con, {
      DBI::dbExecute(con, sprintf("TRUNCATE TABLE %s.%s", schema_name, cfg$table))
      DBI::dbWriteTable(
        con,
        DBI::Id(schema = schema_name, table = cfg$table),
        df,
        append = TRUE,
        row.names = FALSE
      )
    })

    write_log("Tabela", paste(schema_name, cfg$table, sep = "."), "atualizada com", nrow(df), "linhas.")
    registrar_carga_zip(con, pasta, nome_zip, status = "OK")
    invisible(TRUE)
  }, error = function(e) {
    write_log("Erro ao carregar", nome_zip, "para a pasta", pasta, "->", e$message)
    registrar_carga_zip(con, pasta, nome_zip, status = "ERRO", msg_erro = e$message)
    stop(e)
  })
}

carregar_dominios_cnpj <- function(con, pasta_ref) {
  dominios <- list(
    list(zip = "Cnaes.zip",         table = "cad_atividades",        cols = c("num_atv", "desc_atv")),
    list(zip = "Naturezas.zip",     table = "cad_natureza_juridica", cols = c("num_natureza_juridica", "dsc_natureza_juridica")),
    list(zip = "Qualificacoes.zip", table = "cad_tipo_socio",        cols = c("num_tipo_socio", "desc_tipo_socio")),
    list(zip = "Motivos.zip",       table = "cad_motivo_situcada",   cols = c("cod_motivo_situcada", "dsc_motivo_situcada")),
    list(zip = "Paises.zip",        table = "cad_pais",              cols = c("cod_pais", "nom_pais")),
    list(zip = "Municipios.zip",    table = "cad_municipio",         cols = c("cod_municipio_rfb", "nom_municipio"))
  )

  for (cfg in dominios) {
    processar_zip_para_tabela(con, pasta_ref, cfg)
  }
}

# -----------------------------------------------------------
# Execucao principal
# -----------------------------------------------------------
pasta_ref <- Sys.getenv("CNPJ_PASTA_REF", unset = NA_character_)
if (is.na(pasta_ref) || nchar(pasta_ref) == 0) {
  pasta_ref <- detectar_pasta_mais_recente()
} else {
  write_log("Usando pasta informada via CNPJ_PASTA_REF ->", pasta_ref)
}

carregar_dominios_cnpj(con, pasta_ref)

message("Cargas de dominios do CNPJ finalizadas com sucesso para pasta ", pasta_ref, ".")
