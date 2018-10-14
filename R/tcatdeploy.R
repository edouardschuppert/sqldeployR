#' tcatdeploy
#'
#' Deploy a DMI-TCAT database in an R directory as a csv / tsv file with UTF-8 encoding.
#'
#' @param bin The bin name
#' @param hostname The host name of the database. Default set to localhost
#' @param username The username to access the database. Default set to tcatdbuser
#' @param pass The password to access the database
#' @param database The name of the database. Default set to twittercapture
#' @param extension The chosen extension for the deployment of the database. Possible choices : "csv", "tsv". Default set to a "tsv" format
#' @param path File path. Defaults set to current directory
#' @return Returns a csv / tsv file, with a UTF-8 encoding
#' @export

tcatdeploy <- function(bin,
                      hostname = "localhost",
                      username = "tcatdbuser",
                      pass,
                      database = "twittercapture",
                      deploy = TRUE,
                      extension = "tsv",
                      path = "./") {

  # Formating path
  if (stringr::str_detect(path, "/$") == FALSE) path <- paste0(path, "/")

  # Connection to database
  conn <- RMySQL::dbConnect(RMySQL::MySQL(),
                    dbname = database,
                    user = username,
                    password = pass,
                    host = hostname,
                    encoding = "utf-8")

  DBI::dbGetQuery(conn,"set names utf8")

  conn_table <- dplyr::tbl(conn, paste0(bin, "_tweets"))

    # Sending request & processing data
    extract <- conn_table %>%
      dplyr::mutate(id = as.character(id),
                    from_user_id = as.character(from_user_id),
                    retweet_id = as.character(retweet_id),
                    quoted_status_id = as.character(quoted_status_id),
                    to_user_id = as.character(to_user_id),
                    in_reply_to_status_id = as.character(in_reply_to_status_id)) %>%
      dplyr::select(-withheld_scope, -withheld_copyright, -from_user_utcoffset, -from_user_timezone, -from_user_withheld_scope,
                    -geo_lat, -geo_lng) %>%
      dplyr::collect() %>%
      dplyr::mutate(created_at = lubridate::as_datetime(created_at),
                    from_user_created_at = lubridate::as_datetime(from_user_created_at)) %>%
      dplyr::select(id, created_at, from_user_id, from_user_name, from_user_realname, text, lang, from_user_lang, retweet_count, favorite_count, location, source,
                    retweet_id, to_user_id, to_user_name, in_reply_to_status_id, quoted_status_id, from_user_followercount, from_user_friendcount, from_user_favourites_count,
                    from_user_tweetcount, from_user_description,  from_user_verified, from_user_created_at, from_user_listed, from_user_url, from_user_profile_image_url,
                    filter_level, possibly_sensitive) %>%
      dplyr::arrange(plyr::desc(created_at)) %>%
      dplyr::mutate(is_retweet = stringr::str_detect(text, rex::rex(start, "RT @"))) %>%
      dplyr::mutate(retweet_from_user_name = stringr::str_extract(text, rex::rex("RT ", "@", any_non_blanks %or% any_non_spaces, maybe(punct) %or% space %or% end))) %>%
      dplyr::mutate(retweet_from_user_name = stringr::str_remove_all(retweet_from_user_name,"RT ")) %>%
      dplyr::mutate(retweet_from_user_name = stringr::str_remove_all(retweet_from_user_name,"@")) %>%
      dplyr::mutate(retweet_from_user_name = stringr::str_remove_all(retweet_from_user_name,"[[:space:]]*")) %>%
      dplyr::mutate(retweet_from_user_name = stringr::str_remove_all(retweet_from_user_name,"(?:(?::|,)|;)")) %>%
      dplyr::mutate(retweet_from_user_name = stringr::str_remove_all(retweet_from_user_name,"\\)"))


    DBI::dbDisconnect(conn)



    # Case 1 : updating
    if (file.exists(paste0(path, bin, "_datas.", extension)) == TRUE) {

      # Load old datas
      if (extension == "tsv") {
        BDD_old <- readr::read_tsv(paste0(path, bin, "_datas.tsv"), col_types = readr::cols(
          .default = readr::col_character(),
          id = readr::col_character(),
          created_at = readr::col_datetime(format = ""),
          from_user_id = readr::col_character(),
          to_user_id = readr::col_character(),
          in_reply_to_status_id = readr::col_character(),
          from_user_created_at = readr::col_datetime(format = ""),
          retweet_id = readr::col_character()
        ))
      }
      if (extension == "csv") {
        BDD_old <- readr::read_csv(paste0(path, bin, "_datas.csv"), col_types = readr::cols(
          .default = readr::col_character(),
          id = readr::col_character(),
          created_at = readr::col_datetime(format = ""),
          from_user_id = readr::col_character(),
          to_user_id = readr::col_character(),
          in_reply_to_status_id = readr::col_character(),
          from_user_created_at = readr::col_datetime(format = ""),
          retweet_id = readr::col_character()
        ))
      }

      # Isolating new tweets
      new_tweets <- extract %>%
        dplyr::anti_join(BDD_old, by = "id")

      # Updating database
      BDD <- rbind(new_tweets, BDD_old) %>%
        dplyr::distinct(id, .keep_all = TRUE) %>%
        dplyr::arrange(plyr::desc(created_at))

      if (deploy == TRUE) {

        # Save
        if (extension == "tsv") {
          readr::write_tsv(BDD, paste0(path, bin, "_datas.tsv"))
        }
        if (extension == "csv") {
          readr::write_csv(BDD, paste0(path, bin, "_datas.csv"))
        }

        print(paste0("TCAT ", bin, " deployed in ", path, bin, "_datas.", extension))

      }

    }

    # Case 2 : creation
    if (file.exists(paste0(path, bin, "_datas.", extension)) == FALSE) {

      if (deploy == TRUE) {

        # Save
        if (extension == "tsv") {
          readr::write_tsv(extract, paste0(path, bin, "_datas.tsv"))
        }
        if (extension == "csv") {
          readr::write_csv(extract, paste0(path, bin, "_datas.csv"))
        }

        print(paste0("TCAT ", bin, " deployed in ", path, bin, "_datas.", extension))

      }

      BDD <- extract

    }

    if (deploy == FALSE) {

      print(paste0("TCAT ", bin, " NOT deployed as file."))

    }

    BDD

  }

# Alléger par anti_join
# Ajouter extension Rdata
