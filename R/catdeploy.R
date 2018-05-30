#' catdeploy
#'
#' Deploy a TCAT database as a csv/tsv file, with an UTF-8 encoding
#'
#' @param bin The bin name
#' @param hostname The host of the database. Default set to localhost
#' @param username The name of the database. Default set to tcatdbuser
#' @param pass Password of the database
#' @param database The name of the database. Default set to twittercapture
#' @param extension What kind of file to you wand to work with ? Default set to a tsv format
#' @param path Path of the data file. Defaults set to current directory
#' @return Return csv/tsv file, with an UTF-8 encoding
#' @export

catdeploy <- function(bin,
                      hostname = "localhost",
                      username = "tcatdbuser",
                      pass,
                      database = "twittercapture",
                      extension = "tsv",
                      path = "./") {

  # Connection to MySQL
  conn <- RMySQL::dbConnect(RMySQL::MySQL(),
                    dbname = database,
                    user = username,
                    password = pass,
                    host = hostname,
                    encoding = "utf-8")

  # Extraction of datas
  DBI::dbGetQuery(conn,"set names utf8")
  extract <- DBI::dbGetQuery(conn, paste0("SELECT
                                    CONVERT(id, CHAR),
                                    created_at,
                                    text,
                                    from_user_name,
                                    CONVERT(from_user_id, CHAR),
                                    from_user_lang,
                                    CONVERT(lang, CHAR),
                                    CONVERT(to_user_id, CHAR),
                                    to_user_name,
                                    CONVERT(in_reply_to_status_id, CHAR),
                                    CONVERT(source, CHAR),
                                    location,
                                    CONVERT(from_user_tweetcount, UNSIGNED INTEGER),
                                    CONVERT(from_user_followercount, UNSIGNED INTEGER),
                                    CONVERT(from_user_friendcount, UNSIGNED INTEGER),
                                    CONVERT(from_user_listed, UNSIGNED INTEGER),
                                    from_user_realname,
                                    CONVERT(from_user_timezone, CHAR),
                                    from_user_description,
                                    CONVERT(from_user_url, CHAR),
                                    CONVERT(from_user_verified, SIGNED INTEGER),
                                    CONVERT(from_user_profile_image_url, CHAR),
                                    from_user_created_at,
                                    CONVERT(from_user_favourites_count, UNSIGNED INTEGER),
                                    geo_lat,
                                    geo_lng,
                                    CONVERT(retweet_id, CHAR),
                                    CONVERT(retweet_count, UNSIGNED INTEGER),
                                    CONVERT(favorite_count, UNSIGNED INTEGER),
                                    CONVERT(filter_level, CHAR)
                                    FROM ",bin,"_tweets;")) %>%
    dplyr::mutate(created_at = lubridate::as_datetime(created_at),
                  from_user_created_at = lubridate::as_datetime(from_user_created_at))
  DBI::dbDisconnect(conn)

  # Formating datas
  extract <- extract %>%
    dplyr::rename(id = "CONVERT(id, CHAR)") %>%
    dplyr::rename(from_user_id = "CONVERT(from_user_id, CHAR)") %>%
    dplyr::rename(lang = "CONVERT(lang, CHAR)") %>%
    dplyr::rename(to_user_id = "CONVERT(to_user_id, CHAR)") %>%
    dplyr::rename(in_reply_to_status_id = "CONVERT(in_reply_to_status_id, CHAR)") %>%
    dplyr::rename(source = "CONVERT(source, CHAR)") %>%
    dplyr::rename(from_user_tweetcount = "CONVERT(from_user_tweetcount, UNSIGNED INTEGER)") %>%
    dplyr::rename(from_user_friendcount = "CONVERT(from_user_friendcount, UNSIGNED INTEGER)") %>%
    dplyr::rename(from_user_followercount = "CONVERT(from_user_followercount, UNSIGNED INTEGER)") %>%
    dplyr::rename(from_user_listed = "CONVERT(from_user_listed, UNSIGNED INTEGER)") %>%
    dplyr::rename(from_user_timezone = "CONVERT(from_user_timezone, CHAR)") %>%
    dplyr::rename(from_user_url = "CONVERT(from_user_url, CHAR)") %>%
    dplyr::rename(from_user_verified = "CONVERT(from_user_verified, SIGNED INTEGER)") %>%
    dplyr::rename(from_user_profile_image_url = "CONVERT(from_user_profile_image_url, CHAR)") %>%
    dplyr::rename(from_user_favourites_count = "CONVERT(from_user_favourites_count, UNSIGNED INTEGER)") %>%
    dplyr::rename(retweet_id = "CONVERT(retweet_id, CHAR)") %>%
    dplyr::rename(filter_level = "CONVERT(filter_level, CHAR)") %>%
    dplyr::rename(favorite_count = "CONVERT(favorite_count, UNSIGNED INTEGER)") %>%
    dplyr::rename(retweet_count = "CONVERT(retweet_count, UNSIGNED INTEGER)") %>%
    dplyr::arrange(plyr::desc(created_at)) %>%
    dplyr::mutate(is.retweet = stringr::str_detect(text, rex::rex("RT ", "@", any_non_blanks %or% any_non_spaces, maybe(punct) %or% space %or% end))) %>%
    dplyr::mutate(retweet_from_user_name = stringr::str_extract(text, rex::rex("RT ", "@", any_non_blanks %or% any_non_spaces, maybe(punct) %or% space %or% end))) %>%
    dplyr::mutate(retweet_from_user_name = stringr::str_remove_all(retweet_from_user_name,"RT ")) %>%
    dplyr::mutate(retweet_from_user_name = stringr::str_remove_all(retweet_from_user_name,"@")) %>%
    dplyr::mutate(retweet_from_user_name = stringr::str_remove_all(retweet_from_user_name,"[[:space:]]*")) %>%
    dplyr::mutate(retweet_from_user_name = stringr::str_remove_all(retweet_from_user_name,"(?:(?::|,)|;)")) %>%
    dplyr::mutate(retweet_from_user_name = stringr::str_remove_all(retweet_from_user_name,"\\)"))

  # Formating path
  if (stringr::str_detect(path, "/$") == FALSE) path <- paste0(path, "/")

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

    # Save
    if (extension == "tsv") {
      readr::write_tsv(BDD, paste0(path, bin, "_datas.tsv"))
    }
    if (extension == "csv") {
      readr::write_csv(BDD, paste0(path, bin, "_datas.csv"))
    }

  }

  # Case 2 : creation
  if (file.exists(paste0(path, bin, "_datas.", extension)) == FALSE) {

    # Save
    if (extension == "tsv") {
      readr::write_tsv(extract, paste0(path, bin, "_datas.tsv"))
    }
    if (extension == "csv") {
      readr::write_csv(extract, paste0(path, bin, "_datas.csv"))
    }

  }

  print(paste0("TCAT ", bin, " deployed in ", path, bin, "_datas.", extension))

}

# Ajouter choose_periode pour l'importation (selection SQL)
