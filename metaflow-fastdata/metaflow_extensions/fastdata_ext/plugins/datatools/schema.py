"""
Utilities for handling schemas
"""

ffi = None
MD = None
_is_md_import_complete = False


def lazy_import_MD():
    global _is_md_import_complete, MD, ffi
    if not _is_md_import_complete:
        from .md import MD, ffi

        _is_md_import_complete = True


def make_c_schema(name, typ, children=None, child_objects=None):
    """
    Make a c_schema from arrow type information

    """
    lazy_import_MD()

    c_schema = ffi.new("struct ArrowSchema*")
    c_schema.release = MD.md_noop_release_schema
    c_name = ffi.new("char []", name.encode())
    c_type = ffi.new("char []", typ.encode())
    c_schema.name = c_name
    c_schema.format = c_type
    c_schema.children = ffi.NULL
    c_schema.n_children = 0
    c_schema.flags = 2  # set nullable
    objects = [c_name, c_type]
    if children is not None:
        c_children = ffi.new("struct ArrowSchema*[]", len(children))
        c_schema.children = c_children
        c_schema.n_children = len(children)
        for i in range(len(children)):
            c_schema.children[i] = children[i]
            objects += [c_children] + children + child_objects

    return c_schema, objects
