def format_set(*args):
    """
    Format arguments for exceptions
    """
    return ", ".join("'%s'" % i for l in args for i in list(l))
