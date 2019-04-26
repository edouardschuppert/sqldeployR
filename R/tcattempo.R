#' tcattempo
#'
#' Deploy a DMI-TCAT database in an R directory as a csv / tsv file with UTF-8 encoding.
#'
#' @importFrom rlang enquo
#' @importFrom stringr str_detect
#' @importFrom RMariaDB dbConnect
#' @importFrom RMariaDB MariaDB
#' @importFrom RMariaDB dbExecute
#' @importFrom DBI dbGetQuery
#' @importFrom DBI dbDisconnect
#' @import dplyr
#' @importFrom stringr str_remove_all
#' @importFrom stringr str_extract
#' @importFrom lubridate as_datetime
#' @import readr
#' @param bin The bin name
#' @param hostname The host name of the database. Default set to localhost
#' @param username The username to access the database. Default set to tcatdbuser
#' @param pass The password to access the database
#' @param database The name of the database. Default set to twittercapture
#' @param coltime The column on which time filtering is performed. Default set on "created_at"
#' @param startday The start day of the sample
#' @param endday The end day of the sample. Default set on today
#' @param starttime The start time of the sample. Default set to 00:00:00
#' @param endtime The end time of the sample. Default set to 23:59:59
#' @param deploy Save the datas as a file. Default set to TRUE
#' @param extension The chosen extension for the deployment of the database. Possible choices : "csv", "tsv", "rda". Default set to a "rda" format
#' @param path File path. Defaults set to current directory
#' @return Returns a csv / tsv file, with a UTF-8 encoding
#' @export

tcattempo <- function(bin,
                      hostname = "localhost",
                      username = "tcatdbuser",
                      pass,
                      database = "twittercapture",
                      coltime = created_at,
                      startday,
                      endday = Sys.Date(),
                      starttime = "00:00:00",
                      endtime = "23:59:59",
                      deploy = TRUE,
                      extension = "rda",
                      path = "temp/") {

  coltime <- enquo(coltime)
  start_coltime <- paste(startday, starttime)
  end_coltime <- paste(endday, endtime)

  # Formating path & creating directory if necessary
  if (str_detect(path, "/$") == FALSE) path <- paste0(path, "/")
  if (file.exists(path) == FALSE) dir.create(path)

  # Connection to database
  conn <- dbConnect(MariaDB(),
                    dbname = database,
                    user = username,
                    password = pass,
                    host = hostname,
                    encoding = "utf-8")

  dbExecute(conn, "set names utf8")

  conn_table <- tbl(conn, from = paste0(bin, "_tweets"))

  # Sending request & processing data
  extract <- conn_table %>%
    filter(coltime > start_coltime & coltime < end_coltime) %>%
    select(-.data$withheld_scope, -.data$withheld_copyright, -.data$from_user_utcoffset, -.data$from_user_timezone, -.data$from_user_withheld_scope, -.data$geo_lat, -.data$geo_lng) %>%
    collect() %>%
    mutate(id = as.character(id),
           from_user_id = as.character(.data$from_user_id),
           retweet_id = as.character(.data$retweet_id),
           quoted_status_id = as.character(.data$quoted_status_id),
           to_user_id = as.character(.data$to_user_id),
           in_reply_to_status_id = as.character(.data$in_reply_to_status_id)) %>%
    mutate(created_at = as_datetime(.data$created_at),
           from_user_created_at = as_datetime(.data$from_user_created_at)) %>%
    select(.data$id, .data$created_at, .data$from_user_id, .data$from_user_name, .data$from_user_realname, .data$text, .data$lang, .data$from_user_lang, .data$retweet_count,
           .data$favorite_count, .data$location, .data$source, .data$retweet_id, .data$to_user_id, .data$to_user_name, .data$in_reply_to_status_id, .data$quoted_status_id,
           .data$from_user_followercount, .data$from_user_friendcount, .data$from_user_favourites_count, .data$from_user_tweetcount, .data$from_user_description,
           .data$from_user_verified, .data$from_user_created_at, .data$from_user_listed, .data$from_user_url, .data$from_user_profile_image_url, .data$filter_level,
           .data$possibly_sensitive) %>%
    arrange(desc(.data$created_at)) %>%
    mutate(is_retweet = str_detect(.data$text, "^RT @")) %>%
    mutate(retweet_from_user_name = str_extract(.data$text, "RT @(?:[^[:blank:]]*|[^[:space:]]*)(?:(?:(?:[[:punct:]])?|[[:space:]])|$)")) %>%
    mutate(retweet_from_user_name = str_remove_all(.data$retweet_from_user_name,"RT ")) %>%
    mutate(retweet_from_user_name = str_remove_all(.data$retweet_from_user_name,"@")) %>%
    mutate(retweet_from_user_name = str_remove_all(.data$retweet_from_user_name,"[[:space:]]*")) %>%
    mutate(retweet_from_user_name = str_remove_all(.data$retweet_from_user_name,"(?:(?::|,)|;)")) %>%
    mutate(retweet_from_user_name = str_remove_all(.data$retweet_from_user_name,"\\)"))

    dbDisconnect(conn)

    # Case 1 : updating
    if (file.exists(paste0(path, bin, "_datas.", extension)) == TRUE) {

      # Load old datas
      if (extension == "tsv") {
        BDD_old <- read_tsv(paste0(path, bin, "_datas.tsv"), col_types = cols(
          .default = col_character(),
          id = col_character(),
          created_at = col_datetime(format = ""),
          from_user_id = col_character(),
          to_user_id = col_character(),
          in_reply_to_status_id = col_character(),
          from_user_created_at = col_datetime(format = ""),
          retweet_id = col_character()
        ))
      }
      if (extension == "csv") {
        BDD_old <- read_csv(paste0(path, bin, "_datas.csv"), col_types = cols(
          .default = col_character(),
          id = col_character(),
          created_at = col_datetime(format = ""),
          from_user_id = col_character(),
          to_user_id = col_character(),
          in_reply_to_status_id = col_character(),
          from_user_created_at = col_datetime(format = ""),
          retweet_id = col_character()
        ))
      }
      if (extension == "rda") {
        load(paste0(path, bin, "_datas.rda"))
        BDD_old <- BDD
        rm(BDD)
      }

      # Isolating new tweets
      new_tweets <- extract %>%
        anti_join(BDD_old, by = "id")

      # Updating database
      BDD <- rbind(new_tweets, BDD_old) %>%
        distinct(.data$id, .keep_all = TRUE) %>%
        arrange(desc(.data$created_at))

      if (deploy == TRUE) {

        # Save
        if (extension == "tsv") {
          write_tsv(BDD, paste0(path, bin, "_datas.tsv"))
        }
        if (extension == "csv") {
          write_csv(BDD, paste0(path, bin, "_datas.csv"))
        }
        if (extension == "rda") {
          save(BDD, file = paste0(path, bin, "_datas.rda"))
        }

        print(paste0("TCAT ", bin, " deployed in ", path, bin, "_datas.", extension))

      }

    }

    # Case 2 : creation
    if (file.exists(paste0(path, bin, "_datas.", extension)) == FALSE) {

      BDD <- extract

      if (deploy == TRUE) {

        # Save
        if (extension == "tsv") {
          write_tsv(BDD, paste0(path, bin, "_datas.tsv"))
        }
        if (extension == "csv") {
          write_csv(BDD, paste0(path, bin, "_datas.csv"))
        }
        if (extension == "rda") {
          save(BDD, file = paste0(path, bin, "_datas.rda"))
        }

        print(paste0("TCAT ", bin, " deployed in ", path, bin, "_datas.", extension))

      }

    }

    if (deploy == FALSE) {

      print(paste0("TCAT ", bin, " NOT deployed as file."))

    }

    BDD

  }
