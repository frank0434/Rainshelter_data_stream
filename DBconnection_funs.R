
# all the functions that we need 
# copied from Hymmi's work for the ShinyClimate project 
# modified by Frank 

library(DBI)
library(dbplyr)
library(dplyr)
library(RPostgreSQL)

#' setupDBConn
#'
#' @param projectName 
#' @param readSecretFromLocal 
#'
#' @importFrom DBI dbConnect
#' @export
setupDBConn <- function(projectName = "Rainshelter_SWC", readSecretFromLocal = TRUE) {

  ### Setup database connection
  cat("Setting up database connection\n")
  
  #set paths
  if(readSecretFromLocal){
    secretPath <- Sys.getenv("secret_path")
  } else {
    secretPath <- "/input/projects/ShinyClimateChange/Secrets/"
  }
  
  tryCatch(source(paste0(secretPath,"credentials.R")),
           error = function(e) {print("Failed to load secret file")})
  dbcred_write <- get(projectName)
  
  tryCatch(
    conn <- dbConnect(
      RPostgreSQL::PostgreSQL(), 
      dbname = dbcred_write$dbname,
      host = dbcred_write$host,
      user = dbcred_write$username,
      password = dbcred_write$password
    ),
    error = function(e) {print("Failed to connect to db")}
  )
  return(conn)
}

#' writeTableToDB
#'
#' @param conn
#' @param tbl_name
#' @param pk
#' @param df
#' 
#' @import DBI
#' @export
writeTableToDB <- function(conn, tbl_name, pk, df) {
  ### Wipe out table if exists
  if(tbl_name %in% dbListTables(conn)) {
    cat(paste0("Appeding existing ", tbl_name, " table \n"))
  
    }
  
  ### Write table
  cat(paste0("Writing table ", tbl_name, " \n"))
  dbWriteTable(conn, tbl_name, df, row.names = FALSE)
  
  ### Set Primary keys
  sql <- paste0('ALTER TABLE "', tbl_name, '" ADD CONSTRAINT "', tbl_name, '_', pk, '" PRIMARY KEY ("', pk, '");')
  dbExecute(conn, sql)
}
