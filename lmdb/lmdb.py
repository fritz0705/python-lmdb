#!/usr/bin/env python3
# coding: utf-8

import pickle
import os
import os.path
import ctypes
import ctypes.util

MDB_RDONLY = 0x20000
MDB_NOSYNC = 0x10000
MDB_NOSUBDIR = 0x4000
MDB_FIXEDMAP = 0x01
MDB_WRITEMAP = 0x80000
MDB_MAPASYNC = 0x100000
MDB_NOTLS = 0x200000

MDB_REVERSEKEY = 0x02
MDB_DUPSORT = 0x04
MDB_INTEGERKEY = 0x08
MDB_DUPFIXED = 0x10
MDB_INTEGERDUP = 0x20
MDB_REVERSEDUP = 0x40
MDB_CREATE = 0x40000

MDB_NOOVERWRITE = 0x10
MDB_NODUPDATA = 0x20
MDB_CURRENT = 0x40
MDB_RESERVE = 0x10000
MDB_APPEND = 0x20000
MDB_APPENDDUP = 0x40000
MDB_MULTIPLE = 0x80000

MDB_SUCCESS = 0
MDB_KEYEXIST = -30799
MDB_NOTFOUND = -30798
MDB_PAGE_NOTFOUND = -30797
MDB_CORRUPTED = -30796
MDB_PANIC = -30795
MDB_VERSION_MISMATCH = -30794
MDB_INVALID = -30793
MDB_MAP_FULL = -30792
MDB_DBS_FULL = -30791
MDB_READERS_FULL = -30790
MDB_TLS_FULL = -30789
MDB_TXN_FULL = -30788
MDB_CURSOR_FULL = -30787
MDB_PAGE_FULL = -30786
MDB_MAP_RESIZED = -30785
MDB_INCOMPATIBLE = -30784
MDB_BAD_RSLOT = -30783
MDB_BAD_TXN = -30782
MDB_BAD_VALSIZE = -30781

MDB_FIRST = 0
MDB_FIRST_DUP = 1
MDB_GET_BOTH = 2
MDB_GET_BOTH_RANGE = 3
MDB_GET_CURRENT = 4
MDB_GET_MULTIPLE = 5
MDB_LAST = 6
MDB_LAST_DUP = 7
MDB_NEXT = 8
MDB_NEXT_DUP = 9
MDB_NEXT_MULTIPLE = 10
MDB_NEXT_NODUP = 11
MDB_PREV = 12
MDB_PREV_DUP = 13
MDB_PREV_NODUP = 14
MDB_SET = 15
MDB_SET_KEY = 16
MDB_SET_RANGE = 17

class Error(Exception):
	"""Extended Exception class for LMDB exceptions which decodes bytes objects
	by default."""
	def __init__(self, code, msg):
		if isinstance(msg, bytes):
			msg = msg.decode()
		Exception.__init__(self, code, msg)

	@property
	def message(self):
		return self.args[1]

	@property
	def code(self):
		return self.args[0]

class Stat(ctypes.Structure):
	_fields_ = [("ms_psize", ctypes.c_uint),
		("ms_depth", ctypes.c_uint),
		("ms_branch_pages", ctypes.c_size_t),
		("ms_leaf_pages", ctypes.c_size_t),
		("ms_overflow_pages", ctypes.c_size_t),
		("ms_entries", ctypes.c_size_t)]

class EnvInfo(ctypes.Structure):
	_fields_ = [("me_mapaddr", ctypes.c_void_p),
		("me_mapsize", ctypes.c_size_t),
		("me_last_pgno", ctypes.c_size_t),
		("me_last_txnid", ctypes.c_size_t),
		("me_maxreaders", ctypes.c_uint),
		("me_numreaders", ctypes.c_uint)]

class Value(ctypes.Structure):
	_fields_ = [("mv_size", ctypes.c_size_t),
		("mv_data", ctypes.c_void_p)]

	def to_bytes(self):
		return ctypes.string_at(self.mv_data, self.mv_size)
	
	@classmethod
	def from_bytes(cls, b):
		self = cls()
		self.mv_size = len(b)
		self.mv_data = ctypes.cast(ctypes.create_string_buffer(b), ctypes.c_void_p)
		return self

	@classmethod
	def from_object(cls, obj):
		if isinstance(obj, str):
			obj = obj.encode()
		elif isinstance(obj, bytes):
			pass
		else:
			obj = pickle.dumps(obj)
		return cls.from_bytes(obj)

class LibLMDB(object):
	"""Instances of this class represents open shared library handles to a
	liblmdb.so and enables access to it's low-level methods."""

	def __init__(self, lib=None):
		if lib is None:
			lib = ctypes.util.find_library("lmdb")
			if lib is None:
				lib = "liblmdb.so"
		if isinstance(lib, str):
			lib = ctypes.cdll.LoadLibrary(lib)
		elif not isinstance(lib, ctypes.CDLL):
			raise TypeError("Expected lib to be str, ctypes.CDLL or None, got {}".format(type(lib)))
		
		self._lib = lib
		self._monkey_patch_lib(lib)

	@staticmethod
	def _monkey_patch_lib(lib):
		"""Monkey patch supplied ctypes library object to adjust return and argument
		types."""

		# mdb_version
		lib.mdb_version.restype = ctypes.c_char_p
		lib.mdb_version.argtypes = [ctypes.POINTER(ctypes.c_int),
			ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int)]
		
		# mdb_strerror
		lib.mdb_strerror.restype = ctypes.c_char_p
		lib.mdb_strerror.argtypes = [ctypes.c_int]

		# mdb_env_create
		lib.mdb_env_create.restype = ctypes.c_int
		lib.mdb_env_create.argtypes = [ctypes.POINTER(ctypes.c_void_p)]

		# mdb_env_open
		lib.mdb_env_open.restype = ctypes.c_int
		lib.mdb_env_open.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
			ctypes.c_uint, ctypes.c_uint]

		# mdb_env_copy
		lib.mdb_env_copy.restype = ctypes.c_int
		lib.mdb_env_copy.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

		# mdb_env_copyfd
		lib.mdb_env_copyfd.restype = ctypes.c_int
		lib.mdb_env_copyfd.argtypes = [ctypes.c_void_p, ctypes.c_int]

		# mdb_env_stat
		lib.mdb_env_stat.restype = ctypes.c_int
		lib.mdb_env_stat.argtypes = [ctypes.c_void_p, ctypes.POINTER(Stat)]

		# mdb_env_info
		lib.mdb_env_info.restype = ctypes.c_int
		lib.mdb_env_info.argtypes = [ctypes.c_void_p, ctypes.POINTER(EnvInfo)]

		# mdb_env_sync
		lib.mdb_env_sync.restype = ctypes.c_int
		lib.mdb_env_sync.argtypes = [ctypes.c_void_p, ctypes.c_bool]

		# mdb_env_close
		lib.mdb_env_close.argtypes = [ctypes.c_void_p]

		# mdb_env_set_flags
		lib.mdb_env_set_flags.restype = ctypes.c_int
		lib.mdb_env_set_flags.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.c_bool]

		# mdb_env_get_flags
		lib.mdb_env_get_flags.restype = ctypes.c_int
		lib.mdb_env_get_flags.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint)]

		# mdb_env_get_path
		lib.mdb_env_get_path.restype = ctypes.c_int
		lib.mdb_env_get_path.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p)]

		# mdb_env_set_mapsize
		lib.mdb_env_set_mapsize.restype = ctypes.c_int
		lib.mdb_env_set_mapsize.argtypes = [ctypes.c_void_p, ctypes.c_size_t]

		# mdb_env_set_maxreaders
		lib.mdb_env_set_maxreaders.restype = ctypes.c_int
		lib.mdb_env_set_maxreaders.argtypes = [ctypes.c_void_p, ctypes.c_uint]

		# mdb_env_get_maxreaders
		lib.mdb_env_get_maxreaders.restype = ctypes.c_int
		lib.mdb_env_get_maxreaders.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint)]

		# mdb_env_set_maxdbs
		lib.mdb_env_set_maxdbs.restype = ctypes.c_int
		lib.mdb_env_set_maxdbs.argtypes = [ctypes.c_void_p, ctypes.c_uint]

		# mdb_env_get_maxkeysize
		lib.mdb_env_get_maxkeysize.restype = ctypes.c_int
		lib.mdb_env_get_maxkeysize.argtypes = [ctypes.c_void_p]

		# mdb_txn_begin
		lib.mdb_txn_begin.restype = ctypes.c_int
		lib.mdb_txn_begin.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
			ctypes.c_uint, ctypes.POINTER(ctypes.c_void_p)]

		# mdb_txn_env
		lib.mdb_txn_env.restype = ctypes.c_void_p
		lib.mdb_txn_env.argtypes = [ctypes.c_void_p]

		# mdb_txn_commit
		lib.mdb_txn_commit.restype = ctypes.c_int
		lib.mdb_txn_commit.argtypes = [ctypes.c_void_p]

		# mdb_txn_abort
		lib.mdb_txn_abort.restype = None
		lib.mdb_txn_abort.argtypes = [ctypes.c_void_p]

		# mdb_txn_reset
		lib.mdb_txn_reset.restype = None
		lib.mdb_txn_reset.argtypes = [ctypes.c_void_p]

		# mdb_txn_renew
		lib.mdb_txn_renew.restype = ctypes.c_int
		lib.mdb_txn_renew.argtypes = [ctypes.c_void_p]

		# mdb_dbi_open
		lib.mdb_dbi_open.restype = ctypes.c_int
		lib.mdb_dbi_open.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint,
			ctypes.POINTER(ctypes.c_uint)]

		# mdb_stat
		lib.mdb_stat.restype = ctypes.c_int
		lib.mdb_stat.argtypes = [ctypes.c_void_p, ctypes.c_uint,
			ctypes.POINTER(Stat)]

		# mdb_dbi_flags
		lib.mdb_dbi_flags.restype = ctypes.c_int
		lib.mdb_dbi_flags.argtypes = [ctypes.c_void_p, ctypes.c_uint,
			ctypes.POINTER(ctypes.c_uint)]
	
		# mdb_dbi_close
		lib.mdb_dbi_close.restype = None
		lib.mdb_dbi_close.argtypes = [ctypes.c_void_p, ctypes.c_uint]

		# mdb_drop
		lib.mdb_drop.restype = ctypes.c_int
		lib.mdb_drop.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.c_bool]

		# mdb_get
		lib.mdb_get.restype = ctypes.c_int
		lib.mdb_get.argtypes = [ctypes.c_void_p, ctypes.c_uint,
			ctypes.POINTER(Value), ctypes.POINTER(Value)]

		# mdb_put
		lib.mdb_put.restype = ctypes.c_int
		lib.mdb_put.argtypes = [ctypes.c_void_p, ctypes.c_uint,
			ctypes.POINTER(Value), ctypes.POINTER(Value), ctypes.c_uint]
		
		# mdb_del
		lib.mdb_del.restype = ctypes.c_int
		lib.mdb_del.argtypes = [ctypes.c_void_p, ctypes.c_uint,
			ctypes.POINTER(Value), ctypes.POINTER(Value)]

		# mdb_cursor_open
		lib.mdb_cursor_open.restype = ctypes.c_int
		lib.mdb_cursor_open.argtypes = [ctypes.c_void_p, ctypes.c_uint,
			ctypes.POINTER(ctypes.c_void_p)]

		# mdb_cursor_close
		lib.mdb_cursor_close.restype = None
		lib.mdb_cursor_close.argtypes = [ctypes.c_void_p]

		# mdb_cursor_renew
		lib.mdb_cursor_renew.restype = ctypes.c_int
		lib.mdb_cursor_renew.argtypes = [ctypes.c_void_p, ctypes.c_void_p]

		# mdb_cursor_get
		lib.mdb_cursor_get.restype = ctypes.c_int
		lib.mdb_cursor_get.argtypes = [ctypes.c_void_p, ctypes.POINTER(Value),
			ctypes.POINTER(Value), ctypes.c_int]

		# mdb_cursor_put
		lib.mdb_cursor_put.restype = ctypes.c_int
		lib.mdb_cursor_put.argtypes = [ctypes.c_void_p, ctypes.POINTER(Value),
			ctypes.POINTER(Value), ctypes.c_uint]

		# mdb_cursor_del
		lib.mdb_cursor_del.restype = ctypes.c_int
		lib.mdb_cursor_del.argtypes = [ctypes.c_void_p, ctypes.c_uint]

	def version(self):
		"""Obtain version of MDB binding and return 4-tuple of major, minor, patch
		level and version string."""
		major, minor, patch = ctypes.c_int(), ctypes.c_int(), ctypes.c_int()
		res = self._lib.mdb_version(ctypes.pointer(major),
			ctypes.pointer(minor),
			ctypes.pointer(patch)).decode()
		return major.value, minor.value, patch.value, res

	def strerror(self, errno):
		"""Return string error message for provided error number."""
		return self._lib.mdb_strerror(errno).decode()

	def env_create(self):
		"""Create new environment handle and return."""
		val = ctypes.c_void_p()
		err = self._lib.mdb_env_create(ctypes.pointer(val))
		if err != 0:
			raise Error(err, self.strerror(err))
		return val

	def env_open(self, env, path, flags, mode):
		"""Associate environment handle with path."""
		if isinstance(path, str):
			path = path.encode()
		err = self._lib.mdb_env_open(env, path, flags, mode)
		if err != 0:
			raise Error(err, self.strerror(err))
	
	def env_copy(self, env, path):
		"""Copy environment to provided path."""
		err = self._lib.mdb_env_copy(env, path)
		if err != 0:
			raise Error(err, self.strerror(err))

	def env_copyfd(self, env, fd):
		"""Copy environment to provided file descriptor."""
		err = self._lib.mdb_env_copyfd(env, fd)
		if err != 0:
			raise Error(err, self.strerror(err))
	
	def env_stat(self, env):
		"""Return Stat object from environment handle."""
		res = Stat()
		err = self._lib.mdb_env_stat(env, ctypes.pointer(res))
		if err != 0:
			raise Error(err, self.strerror(err))
		return res
	
	def env_info(self, env):
		"""Return EnvInfo object from environment handle."""
		res = EnvInfo()
		err = self._lib.mdb_env_info(env, ctypes.pointer(res))
		if err != 0:
			raise Error(err, self.strerror(err))
		return res
	
	def env_sync(self, env, force):
		"""Sync environment."""
		err = self._lib.mdb_env_sync(env, force)
		if err != 0:
			raise Error(err, self.strerror(err))

	def env_close(self, env):
		"""Destroy environment handle."""
		self._lib.mdb_env_close(env)

	def env_set_flags(self, env, flags, onoff):
		"""Set flags for environment handle."""
		err = self._lib.mdb_env_set_flags(env, flags, onoff)
		if err != 0:
			raise Error(err, self.strerror(err))
	
	def env_get_flags(self, env):
		"""Get flags for environment handle."""
		res = ctypes.c_uint()
		err = self._lib.mdb_env_get_flags(env, ctypes.pointer(res))
		if err != 0:
			raise Error(err, self.strerror(err))
		return res.value

	def env_get_path(self, env):
		"""Return associated path from environment handle."""
		res = ctypes.c_char_p()
		err = self._lib.mdb_env_get_path(env, ctypes.pointer(res))
		if err != 0:
			raise Error(err, self.strerror(err))
		return res.value.decode()

	def env_set_mapsize(self, env, size):
		"""Set mapping size for environment handle."""
		err = self._lib.mdb_env_set_mapsize(env, size)
		if err != 0:
			raise Error(err, self.strerror(err))

	def env_set_maxreaders(self, env, readers):
		"""Set maximum readers for environment handle."""
		err = self._lib.mdb_env_set_maxreaders(env, readers)
		if err != 0:
			raise Error(err, self.strerror(err))
	
	def env_get_maxreaders(self, env):
		"""Get maximum readers for environment handle."""
		res = ctypes.c_uint()
		err = self._lib.mdb_env_get_maxreaders(env, ctypes.pointer(res))
		if err != 0:
			raise Error(err, self.strerror(err))
		return res.value

	def env_set_maxdbs(self, env, dbs):
		"""Set maximum database count for environment handle."""
		err = self._lib.mdb_env_set_maxdbs(env, dbs)
		if err != 0:
			raise Error(err, self.strerror(err))

	def env_get_maxkeysize(self, env):
		"""Get maximum key size for environment handle."""
		res = self._lib.mdb_env_get_maxkeysize(env)
		return res

	def txn_begin(self, env, parent, flags):
		"""Create transaction handle from environment, optional parent transaction,
		and flags."""
		res = ctypes.c_void_p()
		err = self._lib.mdb_txn_begin(env, parent, flags, ctypes.pointer(res))
		if err != 0:
			raise Error(err, self.strerror(err))
		return res

	def txn_env(self, txn):
		"""Return environment handle for transaction handle."""
		res = self._lib.mdb_txn_env(txn)
		return res

	def txn_commit(self, txn):
		"""Commit and invalidate transaction handle."""
		err = self._lib.mdb_txn_commit(txn)
		if err != 0:
			raise Error(err, self.strerror(err))
	
	def txn_abort(self, txn):
		"""Abort and invalidate transaction handle."""
		self._lib.mdb_txn_abort(txn)
	
	def txn_reset(self, txn):
		"""Reset transaction handle."""
		self._lib.mdb_txn_reset(txn)
	
	def txn_renew(self, txn):
		"""Prepare transaction handle for reuse after reset."""
		err = self._lib.mdb_txn_renew(txn)
		if err != 0:
			raise Error(err, self.strerror(err))
	
	def dbi_open(self, txn, name, flags):
		"""Open database handle by transaction handle, optional name and flags."""
		if isinstance(name, str):
			name = name.encode()
		res = ctypes.c_uint()
		err = self._lib.mdb_dbi_open(txn, name, flags, ctypes.pointer(res))
		if err != 0:
			raise Error(err, self.strerror(err))
		return res

	def stat(self, txn, dbi):
		"""Return Stat object for database handle."""
		res = Stat()
		err = self._lib.mdb_stat(txn, dbi, ctypes.pointer(res))
		if err != 0:
			raise Error(err, self.strerror(err))
		return res

	def dbi_flags(self, txn, dbi):
		"""Return flags for database handle."""
		res = ctypes.c_uint()
		err = self._lib.mdb_dbi_flags(txn, dbi, ctypes.pointer(res))
		if err != 0:
			raise Error(err, self.strerror(err))
		return res

	def dbi_close(self, txn, dbi):
		"""Close database handle."""
		self._lib.mdb_dbi_close(txn, dbi)

	def drop(self, txn, dbi, delete=False):
		"""Empty database if delete is False or delete database from enviroment and
		close transaction handle if delete is True."""
		err = self._lib.mdb_drop(txn, dbi, delete)
		if err != 0:
			raise Error(err, self.strerror(err))

	def get(self, txn, dbi, key):
		"""Get items from database handle."""
		res = Value()
		err = self._lib.mdb_get(txn, dbi, ctypes.pointer(key), ctypes.pointer(res))
		if err != 0:
			raise Error(err, self.strerror(err))
		return res

	def put(self, txn, dbi, key, value, flags):
		"""Put item into database."""
		err = self._lib.mdb_put(txn, dbi, ctypes.pointer(key), ctypes.pointer(value), flags)
		if err != 0:
			raise Error(err, self.strerror(err))
	
	def delete(self, txn, dbi, key, value):
		"""Delete item from database."""
		err = self._lib.mdb_del(txn, dbi, ctypes.pointer(key), None if value is None else ctypes.pointer(value))
		if err != 0:
			raise Error(err, self.strerror(err))
	
	def cursor_open(self, txn, dbi):
		res = ctypes.c_void_p()
		err = self._lib.mdb_cursor_open(txn, dbi, ctypes.pointer(res))
		if err != 0:
			raise Error(err, self.strerror(err))
		return res

	def cursor_close(self, cursor):
		self._lib.mdb_cursor_close(cursor)

	def cursor_renew(self, txn, cursor):
		err = self._lib.mdb_cursor_renew(txn, cursor)
		if err != 0:
			raise Error(err, self.strerror(err))
	
	def cursor_get(self, cursor, key, data, op):
		err = self._lib.mdb_cursor_get(cursor, key, data, op)
		if err != 0:
			raise Error(err, self.strerror(err))
		return key, data

	def cursor_put(self, cursor, key, data, flags):
		err = self._lib.mdb_cursor_put(cursor, key, data, flags)
		if err != 0:
			raise Error(err, self.strerror(err))
	
	def cursor_del(self, cursor, flags):
		err = self._lib.mdb_cursor_del(cursor, flags)
		if err != 0:
			raise Error(err, self.strerror(err))
	
class Environment(object):
	"""Instances of this class represents an environment handle and provide higher
	level access to it's properties."""

	_handle = None

	def __init__(self, lib, path=None, flags=None, mode=None):
		self._lib = lib
		self.create()
		if path is not None:
			self.open(path, flags, mode)

	def __del__(self):
		self.close()

	def create(self):
		"""Create environment handle for this environment. This is done on __init__,
		but is necessary after closing."""
		self._handle = self._lib.env_create()

	def open(self, path, flags=None, mode=None):
		"""Associate this environment handle with a database path."""
		if flags is None: flags = 0
		if mode is None: mode = 0o644
		if os.path.isfile(path):
			flags |= MDB_NOSUBDIR
		self._lib.env_open(self._handle, path, flags, mode)

	def close(self):
		"""Close environment handle. You have to recreate an environment handle."""
		if self._handle is None:
			self._lib.env_close(self._handle)
			self._handle = None

	def copy(self, path):
		self._lib.env_copy(self._handle, path)

	def copyfd(self, fd):
		self._lib.env_copyfd(self._handle, fd)
	
	@property
	def stat(self):
		"""Return Stat object for this environment."""
		return self._lib.env_stat(self._handle)

	@property
	def info(self):
		"""Return EnvInfo object for this environment."""
		return self._lib.env_info(self._handle)

	def sync(self, force=False):
		"""Sync this environment."""
		self._lib.env_sync(self._handle, force)

	def set_flags(self, flags, on_off=False):
		"""Set flags for this environment."""
		self._lib.env_set_flags(self._handle, flags, on_off)
	
	def get_flags(self):
		"""Get flags from this environment."""
		return self._lib.env_get_flags(self._handle)

	def get_path(self):
		"""Return associated path for this environment."""
		return self._lib.env_get_path(self._handle)

	def set_mapsize(self, size):
		"""Set mapping size of this environment."""
		self._lib.env_set_mapsize(self._handle, size)
	
	def set_maxreaders(self, maxreaders):
		"""Set maximum readers count for this environment."""
		self._lib.env_set_maxreaders(self._handle, maxreaders)
	
	def get_maxreaders(self):
		"""Get maximum readers count for this environment."""
		return self._lib.env_get_maxreaders(self._handle)

	def set_maxdbs(self, maxdbs):
		"""Set maximum database count for this environment."""
		self._lib.env_set_maxdbs(self._handle, maxdbs)

	def get_maxkeysize(self):
		"""Get maximum key size for this environment."""
		return self._lib.env_get_maxkeysize(self._handle)

	def transaction(self, flags=0, write=True):
		if write is False:
			flags |= MDB_RDONLY
		return Transaction(self, flags=flags)

	begin = transaction

	@property
	def flags(self):
		return self.get_flags(self._handle)

	@flags.setter
	def flags(self, value):
		self.set_flags(self._handle, value, True)

	@property
	def path(self):
		return self.get_path(self._handle)

	@property
	def maxreaders(self):
		return self.get_maxreaders()

	@maxreaders.setter
	def maxreaders(self, value):
		self.set_maxreaders(value)

	@property
	def maxkeysize(self):
		return self.get_maxkeysize()

class Transaction(object):
	"""Instances of Transaction represents an open transaction."""

	_primary_database = None
	_handle = None

	def __init__(self, env, db=None, parent=None, flags=0, lib=None):
		if lib is None:
			lib = env._lib
		self._lib = lib
		self._primary_db = db
		self.env = env
		self.begin(parent, flags)

	def __del__(self):
		self.abort()

	def __enter__(self):
		if self._handle is None:
			self.begin()
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		if exc_type is None:
			self.commit()
		else:
			self.abort()
	
	def begin(self, parent=None, flags=0):
		"""Begin new transaction by allocating a transaction handle."""
		if self._handle is None:
			self._handle = self._lib.txn_begin(self.env._handle,
				parent._handle if parent is not None else None,
				flags)

	def transaction(self, flags=0):
		"""Return new sub-transaction from this transaction."""
		return Transaction(self.env, self, flags)

	def commit(self):
		"""Commit this transaction. After committing it you have to rebegin it."""
		if self._handle is not None:
			self._close_databases()
			self._lib.txn_commit(self._handle)
			self._handle = None
	
	def abort(self):
		"""Abort this transaction. After aborting it you have to rebegin it."""
		if self._handle is not None:
			self._close_databases()
			self._lib.txn_abort(self._handle)
			self._handle = None

	def reset(self):
		"""Reset this transaction."""
		self._close_databases()
		self._lib.txn_reset(self._handle)

	def renew(self):
		"""Renew this transaction after resetting it."""
		self._lib.txn_renew(self._handle)

	def database(self, name=None, flags=0):
		"""Return database object for the associated environment."""
		return Database(self, name, flags)

	def cursor(self):
		return Cursor(self)

	def __setitem__(self, key, value):
		self.primary_database[key] = value

	def __getitem__(self, key):
		return self.primary_database[key]

	def __delitem__(self, key):
		del self.primary_database[key]

	def __contains__(self, key):
		return key in self.primary_database

	def _close_databases(self):
		if self._primary_database is not None:
			self.primary_database.close()
			self._primary_database = None

	@property
	def primary_database(self):
		"""Get primary database for the associated environment."""
		if self._primary_database is None:
			self._primary_database = self.database(self._primary_db)
		return self._primary_database

class Database(object):
	"""Instances of Database represents database handles which are
	associated with a transaction and an environment."""

	def __init__(self, transaction, name, flags=0, lib=None):
		if lib is None:
			lib = transaction._lib
		self._lib = lib
		self.transaction = transaction
		self._handle = self._lib.dbi_open(transaction._handle, name, flags)

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		self.close()

	def __del__(self):
		self.close()

	def cursor(self):
		return Cursor(self)

	def stat(self):
		"""Return Stat for database handle."""
		return self._lib.stat(self.transaction._handle, self._handle)

	def flags(self):
		"""Return flags for database handle."""
		return self._lib.dbi_flags(self.transaction._handle, self._handle)

	def close(self):
		"""Close this database handle."""
		self._lib.dbi_close(self.transaction._handle, self._handle)

	def empty(self):
		"""Empty this database."""
		self._lib.drop(self.transaction._handle, self._handle, False)

	def drop(self):
		"""Drop this database and close it."""
		self._lib.drop(self.transaction._handle, self._handle, True)

	def get(self, key):
		"""Get item from database."""
		if not isinstance(key, Value):
			key = Value.from_object(key)
		res = self._lib.get(self.transaction._handle, self._handle, key)
		return res.to_bytes()

	def put(self, key, value, flags=0):
		"""Put item into database."""
		if not isinstance(key, Value):
			key = Value.from_object(key)
		if not isinstance(value, Value):
			value = Value.from_object(value)
		self._lib.put(self.transaction._handle, self._handle, key, value, flags)

	def delete(self, key, value=None):
		"""Delete item from database."""
		if not isinstance(key, Value):
			key = Value.from_object(key)
		if value is not None and not isinstance(value, Value):
			value = Value.from_object(value)
		self._lib.delete(self.transaction._handle, self._handle, key, value)

	def __getitem__(self, key):
		try:
			return self.get(key)
		except Error as e:
			if e.code == MDB_NOTFOUND:
				raise KeyError(key)
			else:
				raise

	def __setitem__(self, key, value):
		self.put(key, value)

	def __contains__(self, key):
		try:
			self[key]
		except KeyError:
			return False
		return True

	def __delitem__(self, key):
		try:
			self.delete(key)
		except Error as e:
			if e.code == MDB_NOTFOUND:
				raise KeyError(key)
			else:
				raise

	@property
	def env(self):
		return self.transaction.env

class Cursor(object):
	_handle = None

	def __init__(self, txn_or_db, lib=None):
		if isinstance(txn_or_db, Database):
			db = txn_or_db
			txn = db.transaction
		elif isinstance(txn_or_db, Transaction):
			txn = txn_or_db
			db = txn.primary_database
		else:
			raise TypeError("Expected txn_or_db to be Transaction or Database, got {}".format(type(txn_or_db)))
		if lib is None:
			lib = db._lib
		self._lib = lib
		self.open(txn, db)
	
	def open(self, txn, db):
		if self._handle is not None:
			self.close()
		self._handle = self._lib.cursor_open(txn._handle, db._handle)

	def close(self):
		if self._handle is not None:
			self._lib.cursor_close(self._handle)
			self._handle = None
	
	def renew(self, txn):
		self._lib.cursor_renew(txn._handle, self._handle)

	def get(self, op, key=None, data=None):
		if not isinstance(key, Value):
			key = Value.from_object(key)
		if not isinstance(data, Value):
			data = Value.from_object(data)
		key, value = self._lib.cursor_get(self._handle, key, data, op)
		return key.to_bytes(), data.to_bytes()

	def put(self, key, data, flags=0):
		if not isinstance(key, Value):
			key = Value.from_object(key)
		if not isinstance(data, Value):
			data = Value.from_object(data)
		self._lib.cursor_put(self._handle, key, data, flags)

	def delete(self, flags=0):
		self._lib.cursor_del(self._handle, flags)
	
	def next(self):
		return self.get(MDB_NEXT)

	def prev(self):
		return self.get(MDB_PREV)

	def first(self):
		return self.get(MDB_FIRST)

	def last(self):
		return self.get(MDB_LAST)

	def __del__(self):
		self.close()

	def __iter__(self):
		return self

	def __next__(self):
		try:
			return self.get(MDB_NEXT)
		except Error as e:
			if e.code == MDB_NOTFOUND:
				raise StopIteration()
			raise

try:
  lib = LibLMDB(os.environ.get("LMDB_SO_PATH"))
except lmdb.Error:
  pass

