#  Taking first steps to go through the data Tyler generated
#  per instructions via Slack


readRDS(file = nais_ps/data/works.rds)
getwd()  # [1] "C:/Users/Owner/Documents/Research/R_Projects/nais_ps"

readRDS(file = /data/works.rds)  # Error: unexpected '/' in "readRDS(file = /"
readRDS(file = data/works.rds)  # Error: object 'works.rds' not found
readRDS(file = works.rds) # Error: object 'works.rds' not found
readRDS(file = works)  # Error: object 'works' not found

readRDS(file = refs)  # Error: object 'refs' not found


# same results if I try to read in "refs.rds"

# But I can SEE them!?!?!


readRDS(nais_ps/data/works.rds)  # Error: object 'nais_ps' not found
