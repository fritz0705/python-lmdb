python-lmdb
===========

Your (L)MDB bindings for Python, written entirely using ctypes, but only for Python 3 (at the moment). Does not use any CPython native extensions.

Usage
-----

    import lmdb
    lib = lmdb.LibLMDB()
    env = lmdb.Environment(lib, "data")
    with env.transaction() as txn:
        txn[b"key"] = b"value"
        print(txn[b"key"])
        del txn[b"key"]

Web API
-------

I have implemented a simple web REST API with Bottle. It supports simple
transactions, setting an item, getting an item, and deleting an item.

    $ gunicorn lmdb.web

You can use the following environment variables to configure the application:

* *LMDB_WEB_LIB*: Path to liblmdb.so
* *LMDB_WEB_DBPATH*: Path to database directory

It supports simple REST endpoints:

* `GET /` Server status overview
* `GET /_simple/<key>` Retrievement of item by key
* `PUT /_simple/<key>` Set an item with request body
* `DELETE /_simple/<key>` Delete an item
* `POST /_trans` Upload transaction and execute it
* `GET /_dump` Return transaction which inserts data

Transactions are uploaded as JSON with the following form:

    {
        "write": true, # optional, by default false
        "steps": [
            {"action": "set", "key": "foo", "value": "bar"},
            {"action": "delete", "key": "foo"},
        ]
    }

