# this file is named coveragerc.py since metaflow does not have a way to package hidden files

[run]
branch = True
parallel = True
concurrency = multiprocessing,thread

[html]
show_contexts = True

[report]
omit =
  /**/site-packages/*
  /**/vendor/*
  /**/vendored/*
  /**/_vendor/*
  /**/build/*

include_namespace_packages = True
show_missing = False
ignore_errors = False
