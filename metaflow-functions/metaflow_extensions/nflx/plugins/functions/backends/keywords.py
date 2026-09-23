# Kwargs that address the backend rather than the decorated function, so that
# `f(data, process=1)` does not hand `process` to the user's code. Shared so
# the backends cannot disagree about which names are theirs.
KEYWORDS = {"process", "params"}
