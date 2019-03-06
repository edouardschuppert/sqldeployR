#' call_db
#'
#' Deploy a table in an R directory, as an RDA file.
#'
#' @importFrom stringr str_detect
#' @importFrom RMariaDB dbConnect
#' @importFrom RMariaDB MariaDB
#' @importFrom dplyr tbl
#' @importFrom dplyr collect
#' @importFrom DBI dbDisconnect
#' @param tableName The table name.
#' @param hostname The host name of the database. Default set to localhost
#' @param username The username to access the database.
#' @param pass The password to access the database
#' @param database The name of the database.
#' @param path File path. Defaults set as "temp"
#' @return Returns a dataframe
#' @export

call_db <- function(tableName,
                    hostname = "localhost",
                    username,
                    pass,
                    database,
                    path = "temp/") {

  if (str_detect(path, "/$") == FALSE) path <- paste0(path, "/")

  if ((paste0(tableName, ".rda") %in% list.files(path)) == TRUE) {

    load(paste0(path, tableName, ".rda"))

  }

  if ((paste0(tableName, ".rda") %in% list.files(path)) == FALSE) {

    conn <- dbConnect(MariaDB(),
                      dbname = database,
                      user = username,
                      password = pass,
                      host = hostname,
                      encoding = "utf-8")

    corpus <- tbl(conn, tableName) %>% collect()

    dbDisconnect(conn)

    save(corpus, file = paste0(path, tableName, ".rda"))

    # Spare RAM
    rm(conn)

  }

  corpus

}

# Rajouter possibilité de ne pas déployer
# Rajouter possibilité de déployer dans un format différent
# Rajouter possibilité de déployer dans d'autres dossiers ou à la racine
