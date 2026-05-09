cat("=== Toy test Fix B: censobr filter on expanded codes ===\n")

# Simulate expand_range for "110002305000001-0010" (10 codes)
expanded <- sprintf("1100023050000%02d", 1:10)
cat("Expanded codes:", length(expanded), "\n")
cat("  ", paste(expanded, collapse=", "), "\n")

# Simulate censobr: only 7 of those 10 actually exist
censobr_codes <- c("110002305000001", "110002305000002", "110002305000003",
                   "110002305000005", "110002305000007", "110002305000008",
                   "110002305000010")  # missing: 004, 006, 009

cat("\nCensobr codes:", length(censobr_codes), "\n")

# Filter expanded by censobr
valid_codes <- expanded[expanded %in% censobr_codes]
cat("Valid codes after filter:", length(valid_codes), "\n")
cat("  ", paste(valid_codes, collapse=", "), "\n")

# What's missing?
missing <- expanded[!expanded %in% censobr_codes]
cat("Missing from censobr:", length(missing), "\n")
cat("  ", paste(missing, collapse=", "), "\n")

# Verify
stopifnot(length(valid_codes) == 7)
stopifnot(length(valid_codes) < length(expanded))
stopifnot(all(valid_codes %in% censobr_codes))
stopifnot(!any(missing %in% censobr_codes))

cat("\nPASS: censobr filter correctly reduces expanded codes\n")
