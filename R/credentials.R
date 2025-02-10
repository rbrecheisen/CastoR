library(R6)


#' @title Class for handling Castor API credentials
#'
#' @field client_id_file_name Client ID file name (default: "castorclientid.txt")
#' @field client_id_file_path Full client ID file path
#' @field client_id Client ID
#' @field client_secret_file_name Client secret file name (default: "castorclientsecret.txt")
#' @field client_secret_file_path Full client secret file path
#' @field client_secret Client secret
#' @field study_name_file_name Study name file name (default: castorstudy.txt")
#' @field study_name_file_path Study name file path
#' @field study_name Study name
#'
#' @export
CastorApiCredentials <- R6Class("CastorApiCredentials",
  public = list(

    client_id_file_name = "castorclientid.txt",
    client_id_file_path = NULL,
    client_id = NULL,
    client_secret_file_name = "castorclientsecret.txt",
    client_secret_file_path = NULL,
    client_secret = NULL,
    study_name_file_name = "castorstudy.txt",
    study_name_file_path = NULL,
    study_name = NULL,

    #' @description Initializes Castor API credentials handler
    #'
    #' @param client_id_file_name Client ID file name
    #' @param client_secret_file_name Client secret file name
    #' @param study_name_file_name Study name file name
    #'
    #' @export
    initialize = function(client_id_file_name = NULL, client_secret_file_name = NULL, study_name_file_name = NULL) {
      if(!is.null(client_id_file_name)) {
        self$client_id_file_name <- client_id_file_name
      }

      self$client_id_file_path <- file.path(path.expand("~"), self$client_id_file_name)
      print(paste0("Using client_id_file_path = ", self$client_id_file_path))

      if(!is.null(client_secret_file_name)) {
        self$client_secret_file_name <- client_secret_file_name
      }

      self$client_secret_file_path <- file.path(path.expand("~"), self$client_secret_file_name)
      print(paste0("Using client_secret_file_path = ", self$client_secret_file_path))

      if(!is.null(study_name_file_name)) {
        self$study_name_file_name = study_name_file_name
      }

      self$study_name_file_path <- file.path(path.expand("~"), self$study_name_file_name)
      print(paste0("Using study_name_file_path = ", self$study_name_file_path))
    },

    #' @description Loads client ID from file
    #'
    #' @return Client ID or NULL if not found
    #'
    #' @export
    load_client_id = function() {
      if(file.exists(self$client_id_file_path)) {
        client_id_file <- file(self$client_id_file_path, "r")
        self$client_id <- readLines(client_id_file)
        close(client_id_file)
        return(self$client_id)
      } else {
        print(paste0(
          "The file '", self$client_id_file_name, "' could not be found. ",
          "Make sure to store the Castor client ID in a text file called '", self$client_id_file_name, "' ",
          "in the '", path.expand("~"), "' directory."
        ))
        return(NULL)
      }
    },

    #' @description Saves client ID to file
    #'
    #' @param client_id Client ID
    #'
    #' @export
    save_client_id = function(client_id) {
      if(!file.exists(self$client_id_file_path)) {
        client_id_file <- file(self$client_id_file_path, "w")
        writeLines(client_id, client_id_file)
        close(client_id_file)
        return(client_id)
      } else {
        print(paste0("Warning: client ID file ", self$client_id_file_path, " already exists"))
        return(NULL)
      }
    },

    #' @description Loads client secret from file
    #'
    #' @return Client secret or NULL if not found
    #'
    #' @export
    load_client_secret = function() {
      if(file.exists(self$client_secret_file_path)) {
        client_secret_file <- file(self$client_secret_file_path, "r")
        self$client_secret <- readLines(client_secret_file)
        close(client_secret_file)
        return(self$client_secret)
      } else {
        print(paste0(
          "The file '", self$client_secret_file_name, "' could not be found. ",
          "Make sure to store the Castor client secret in a text file called '", self$client_secret_file_name, "' ",
          "in the '", path.expand("~"), "' directory."
        ))
        return(NULL)
      }
    },

    #' @description Saves client secret to file
    #'
    #' @param client_secret Client secret
    #'
    #' @export
    save_client_secret = function(client_secret) {
      if(!file.exists(self$client_secret_file_path)) {
        client_secret_file <- file(self$client_secret_file_path, "w")
        writeLines(client_secret, client_secret_file)
        close(client_secret_file)
        return(client_secret)
      } else {
        print(paste0("Warning: client secret file ", self$client_secret_file_path, " already exists"))
        return(NULL)
      }
    },

    #' @description Loads study name from file
    #'
    #' @return Study name or NULL if not found
    #'
    #' @export
    load_study_name = function() {
      if(file.exists(self$study_name_file_path)) {
        study_name_file <- file(self$study_name_file_path, "r")
        self$study_name <- readLines(study_name_file)
        close(study_name_file)
        return(self$study_name)
      }
      else {
        print(paste0(
          "The file '", self$study_name_file_name, "' could not be found. ",
          "Make sure to store the Castor study name in a text file called '", self$study_name_file_name, "'",
          "in the '", path.expand("~"), "' directory."
        ))
      }
    },

    #' @description Saves study name to file
    #'
    #' @param study_name Study name
    #'
    #' @export
    save_study_name = function(study_name) {
      if(!file.exists(self$study_name_file_path)) {
        study_name_file <- file(self$study_name_file_path, "w")
        writeLines(study_name, study_name_file)
        close(study_name_file)
        return(study_name)
      } else {
        print(paste0("Warning: study name file ", self$study_name_file_path, " already exists"))
        return(NULL)
      }
    }
  )
)


# credentials <- CastorApiCredentials$new("id.txt", "secret.txt")
# credentials <- CastorApiCredentials$new()
# id <- credentials$load_client_id()
# if(is.null(id)) {
#   id <- credentials$save_client_id("1234")
#   print(paste0("Saved ID: ", id))
# }
#
# secret <- credentials$load_client_secret()
# if(is.null(secret)) {
#   secret <- credentials$save_client_secret("ABCD")
#   print(paste0("Saved secret: ", secret))
# }
