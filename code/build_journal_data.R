library(data.table)
library(jsonlite)

source("code/read_source_works.R")

works <- build_works()
refs  <- build_refs()

saveRDS(works, "data/works.rds")

ref_sub <- refs[refs$cited_id %in% refs$citing_id,]
saveRDS(ref_sub,  "data/refs.rds")

message("Saved ", nrow(works), " works and ", nrow(ref_sub), " reference edges.")
