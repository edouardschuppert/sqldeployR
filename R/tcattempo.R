#' tcattempo
#'
#' Deploy a DMI-TCAT database in an R directory, as a csv / tsv file, with UTF-8 encoding and a period selection.
#'
#' @param bin The bin name
#' @param hostname The host name of the database. Default set to localhost
#' @param username The username to access the database. Default set to tcatdbuser
#' @param pass The password to access the database
#' @param database The name of the database. Default set to twittercapture
#' @param extension The chosen extension for the deployment of the database. Possible choices : "csv", "tsv". Default set to a "tsv" format
#' @param path File path. Defaults set to current directory
#' @param startday The first day of the period you want to select
#' @param endday The last day of the period you want to select
#' @param starttime The time of the first day of the period you want to select. Default set to 00:00:00
#' @param endtime The time of the last day of the period you want to select. Default set to 23:59:59
#' @return Returns a csv / tsv file, with a UTF-8 encoding
#' @export

tcattempo <- function(bin,
                      hostname = "localhost",
                      username = "tcatdbuser",
                      pass,
                      database = "twittercapture",
                      extension = "tsv",
                      path = "./",
                      startday,
                      endday = Sys.Date(),
                      starttime = "00:00:00",
                      endtime = "23:59:59") {

  # Concatenate datetime
  starttimestamp <- lubridate::as_datetime(paste(startday, starttime))
  endtimestamp <- lubridate::as_datetime(paste(endday, endtime))

  # Connection to database
  conn <- RMariaDB::dbConnect(RMariaDB::MySQL(),
                            dbname = database,
                            user = username,
                            password = pass,
                            host = hostname,
                            encoding = "utf-8")

  DBI::dbGetQuery(conn, "set names utf8")

  conn_table <- dplyr::tbl(conn, paste0(bin, "_tweets"))

  # Sending request & processing data
  extract <- conn_table %>%
    dplyr::mutate(id = as.character(id),
                  from_user_id = as.character(from_user_id),
                  retweet_id = as.character(retweet_id),
                  quoted_status_id = as.character(quoted_status_id),
                  to_user_id = as.character(to_user_id),
                  in_reply_to_status_id = as.character(in_reply_to_status_id)) %>%
    dplyr::filter(created_at > starttimestamp &
                    created_at < endtimestamp) %>%
    dplyr::select(-withheld_scope, -withheld_copyright, -from_user_utcoffset, -from_user_timezone, -from_user_withheld_scope,
                  -geo_lat, -geo_lng) %>%
    dplyr::collect() %>%
    dplyr::mutate(created_at = lubridate::as_datetime(created_at),
                  from_user_created_at = lubridate::as_datetime(from_user_created_at)) %>%
    dplyr::select(id, created_at, from_user_id, from_user_name, from_user_realname, text, lang, from_user_lang, retweet_count, favorite_count, location, source,
           retweet_id, to_user_id, to_user_name, in_reply_to_status_id, quoted_status_id, from_user_followercount, from_user_friendcount, from_user_favourites_count,
           from_user_tweetcount, from_user_description,  from_user_verified, from_user_created_at, from_user_listed, from_user_url, from_user_profile_image_url,
           filter_level, possibly_sensitive)

  DBI::dbDisconnect(conn)

  # Save
  if (file.exists(paste0(path, bin, "_datas.", extension)) == FALSE) {

    if (extension == "tsv") {
      readr::write_tsv(extract, paste0(path, bin, "_datas.tsv"))
    }
    if (extension == "csv") {
      readr::write_csv(extract, paste0(path, bin, "_datas.csv"))
    }

  }

  # Show result message
  print(paste0("TCAT ", bin, " (from ", startday," to ", endday,") deployed in ", path, bin, "_datas.", extension))

}
