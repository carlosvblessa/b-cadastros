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

con <- NULL
con <- tryCatch(
  DBI::dbConnect(
    drv = RPostgres::Postgres(),
    dbname = Sys.getenv("DB_NAME"),
    host = Sys.getenv("DB_HOST"),
    port = port,
    user = Sys.getenv("DB_USER"),
    password = Sys.getenv("DB_PASSWORD")
  ),
  error = function(e) {
    stop(
      "Nao foi possivel conectar ao banco '", Sys.getenv("DB_NAME"), "' em ",
      Sys.getenv("DB_HOST"), ":", Sys.getenv("DB_PORT"),
      " com o usuario fornecido. Erro original: ", e$message
    )
  }
)
if (!DBI::dbIsValid(con)) {
  stop("Conexao ao banco ficou invalida; verifique as credenciais e rede.")
}
on.exit(DBI::dbDisconnect(con), add = TRUE)

test_conn <- tryCatch(
  DBI::dbGetQuery(con, "select current_database() as db, current_user as usr"),
  error = function(e) {
    stop(
      "Consegui abrir a conexao, mas uma query simples falhou (possivel problema de permissao de CONNECT, rede ou pgbouncer). Erro: ",
      e$message
    )
  }
)
message("Conectado em banco=", test_conn$db, " como usuario=", test_conn$usr)

schema_name <- "admb_cads"
schema_exists <- nrow(
  DBI::dbGetQuery(
    con,
    "select 1 from pg_namespace where nspname = $1 limit 1",
    params = list(schema_name)
  )
) > 0

if (!schema_exists) {
  message("Schema '", schema_name, "' nao encontrado; tentando criar.")
  tryCatch({
    DBI::dbExecute(
      con,
      sprintf("CREATE SCHEMA %s", DBI::dbQuoteIdentifier(con, schema_name)),
      immediate = TRUE
    )
  }, error = function(e) {
    DBI::dbDisconnect(con)
    stop(
      "Nao consegui criar o schema '", schema_name,
      "'. Use um usuario com permissao CREATE no database ou crie manualmente e conceda USAGE/CREATE no schema. Erro original: ",
      e$message
    )
  })
} else {
  message("Schema '", schema_name, "' encontrado; pulando criacao.")
}

sql_dir <- file.path("sql", "postgres")
sql_files <- sort(list.files(sql_dir, pattern = "\\.sql$", full.names = TRUE))

if (length(sql_files) == 0) {
  DBI::dbDisconnect(con)
  stop("Nenhum arquivo .sql encontrado em ", sql_dir)
}

for (path in sql_files) {
  message("Aplicando ", basename(path))
  sql <- readr::read_file(path)
  DBI::dbExecute(con, sql, immediate = TRUE) # immediate=TRUE permite multiplos comandos no mesmo arquivo
}

message("Estruturas criadas/validadas com sucesso.")
