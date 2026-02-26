library(data.table)
library(jsonlite)

# ── internal helpers ───────────────────────────────────────────────────────────

# Strip URL prefixes from OpenAlex / DOI / PubMed identifiers
strip_id <- function(x) {
  if (is.null(x) || length(x) == 0L) return(NA_character_)
  x <- as.character(x)
  x <- sub("https://openalex\\.org/",               "", x)
  x <- sub("https://doi\\.org/",                    "", x)
  x <- sub("https://pubmed\\.ncbi\\.nlm\\.nih\\.gov/", "", x)
  x
}

# Coerce NULL / zero-length values to a typed NA
nn <- function(x, type = "character") {
  if (is.null(x) || length(x) == 0L)
    switch(type,
      character = NA_character_,
      integer   = NA_integer_,
      double    = NA_real_,
      logical   = NA
    )
  else x
}

# Extract scalar fields from one work object into a single-row data.table
extract_work_row <- function(w, source_id, source_group) {
  data.table(
    source_id                   = source_id,
    source_group                = source_group,
    work_id                     = strip_id(w$id),
    doi                         = strip_id(nn(w$doi)),
    pmid                        = strip_id(nn(w$ids$pmid)),
    mag                         = nn(w$ids$mag),
    title                       = nn(w$title),
    publication_year            = nn(w$publication_year,            "integer"),
    publication_date            = nn(w$publication_date),
    type                        = nn(w$type),
    language                    = nn(w$language),
    cited_by_count              = nn(w$cited_by_count,              "integer"),
    referenced_works_count      = nn(w$referenced_works_count,      "integer"),
    locations_count             = nn(w$locations_count,             "integer"),
    countries_distinct_count    = nn(w$countries_distinct_count,    "integer"),
    institutions_distinct_count = nn(w$institutions_distinct_count, "integer"),
    fwci                        = nn(w$fwci,                        "double"),
    is_retracted                = nn(w$is_retracted,                "logical"),
    is_paratext                 = nn(w$is_paratext,                 "logical"),
    has_fulltext                = nn(w$has_fulltext,                "logical"),
    oa_is_oa                    = nn(w$open_access$is_oa,           "logical"),
    oa_status                   = nn(w$open_access$oa_status),
    biblio_volume               = nn(w$biblio$volume),
    biblio_issue                = nn(w$biblio$issue),
    biblio_first_page           = nn(w$biblio$first_page),
    biblio_last_page            = nn(w$biblio$last_page),
    primary_topic               = nn(w$primary_topic$display_name),
    primary_topic_subfield      = nn(w$primary_topic$subfield$display_name),
    primary_topic_field         = nn(w$primary_topic$field$display_name),
    primary_topic_domain        = nn(w$primary_topic$domain$display_name),
    primary_source_display_name = nn(w$primary_location$source$display_name),
    primary_source_type         = nn(w$primary_location$source$type),
    primary_source_issn_l       = nn(w$primary_location$source$issn_l),
    created_date                = nn(w$created_date),
    updated_date                = nn(w$updated_date)
  )
}

# Extract citing → cited edge rows from one work object
extract_refs <- function(w) {
  refs <- w$referenced_works
  if (is.null(refs) || length(refs) == 0L) return(NULL)
  data.table(
    citing_id = strip_id(w$id),
    cited_id  = strip_id(as.character(refs))
  )
}

# ── main function ──────────────────────────────────────────────────────────────

#' Read all downloaded page files for one OpenAlex source.
#'
#' @param source_id     OpenAlex source ID (e.g. "S27405227").
#' @param oa_works_dir  Directory containing per-source subdirectories.
#' @param resolved_file Path to resolved_sources.json.
#'
#' @return Named list:
#'   $works      data.table — one row per work, scalar fields only.
#'   $references data.table — edge list of (citing_id, cited_id) pairs.
read_source_works <- function(
    source_id,
    oa_works_dir  = "data/oa_works",
    resolved_file = "data/resolved_sources.json"
) {
  sid <- source_id  # local copy avoids name clash in data.table expressions

  # Look up source group(s) from resolved_sources.json
  resolved     <- fromJSON(resolved_file, simplifyDataFrame = TRUE)
  src_dt       <- as.data.table(resolved$sources)
  grps         <- src_dt[source_id == sid, unique(group)]
  source_group <- paste(sort(grps), collapse = "|")

  # Locate page files
  source_dir <- file.path(oa_works_dir, sid)
  page_files <- sort(list.files(source_dir, pattern = "^page_\\d+\\.json$",
                                full.names = TRUE))
  if (length(page_files) == 0L)
    stop("No page files found in: ", source_dir)

  works_list <- vector("list", length(page_files))
  refs_list  <- vector("list", length(page_files))

  for (i in seq_along(page_files)) {
    page    <- fromJSON(page_files[[i]], simplifyVector = FALSE)
    results <- page$results

    works_list[[i]] <- rbindlist(
      lapply(results, extract_work_row, source_id = sid, source_group = source_group)
    )
    refs_list[[i]] <- rbindlist(lapply(results, extract_refs))
  }

  list(
    works      = rbindlist(works_list),
    references = rbindlist(refs_list)
  )
}

# ── convenience wrappers ────────────────────────────────────────────────────────

#' Build a combined works data.table across all resolved sources.
build_works <- function(
    oa_works_dir  = "data/oa_works",
    resolved_file = "data/resolved_sources.json"
) {
  resolved   <- fromJSON(resolved_file, simplifyDataFrame = TRUE)
  source_ids <- unique(resolved$sources$source_id)
  rbindlist(lapply(source_ids, function(sid) {
    read_source_works(sid, oa_works_dir, resolved_file)$works
  }))
}

#' Build a combined reference edgelist across all resolved sources.
build_refs <- function(
    oa_works_dir  = "data/oa_works",
    resolved_file = "data/resolved_sources.json"
) {
  resolved   <- fromJSON(resolved_file, simplifyDataFrame = TRUE)
  source_ids <- unique(resolved$sources$source_id)
  rbindlist(lapply(source_ids, function(sid) {
    read_source_works(sid, oa_works_dir, resolved_file)$references
  }))
}
