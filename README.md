python-lmdb
===========

Your (L)MDB bindings for Python, written entirely using ctypes, but only for Python 3 (at the moment). Does not use any CPython native extensions.

Usage
-----

    import lmdb
    lib = lmdb.LibLMDB()
    env = lmdb.Environment(lib)
    env.open("database", 0, 0o644)
    with env.transaction() as tr:
        with lmdb.Database(tr, None) as db:
            db["key"] = "value"
