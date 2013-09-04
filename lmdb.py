#!/usr/bin/env python3
# coding: utf-8

import pickle
import ctypes
import ctypes.util

class Error(Exception):
	def __init__(self, msg):
		if isinstance(msg, bytes):
			msg = msg.encode()
		Exception.__init__(self, msg)

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
	def __init__(self, lib=None):
		if lib is None:
			lib = ctypes.util.find_library("lmdb")
			if lib is None:
				raise ValueError("Could not find lmdb shared object")
		if isinstance(lib, str):
			lib = ctypes.cdll.LoadLibrary(lib)
		elif not isinstance(lib, ctypes.CDLL):
			raise TypeError("Expected lib to be str, ctypes.CDLL or None, got {}".format(type(lib)))
		
		self._lib = lib
		self._monkey_patch_lib(lib)

	@staticmethod
	def _monkey_patch_lib(lib):
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
			ctypes.POINTER(Value), ctypes.POINTER(Value)]
		
		# mdb_del
		lib.mdb_del.restype = ctypes.c_int
		lib.mdb_del.argtypes = [ctypes.c_void_p, ctypes.c_uint,
			ctypes.POINTER(Value), ctypes.POINTER(Value)]

	def version(self):
		major, minor, patch = ctypes.c_int(), ctypes.c_int(), ctypes.c_int()
		res = self._lib.mdb_version(ctypes.pointer(major),
			ctypes.pointer(minor),
			ctypes.pointer(patch)).decode()
		return major.value, minor.value, patch.value, res

	def strerror(self, errno):
		return self._lib.mdb_strerror(errno).decode()

	def env_create(self):
		val = ctypes.c_void_p()
		err = self._lib.mdb_env_create(ctypes.pointer(val))
		if err != 0:
			raise Error(self.strerror(err))
		return val

	def env_open(self, env, path, flags, mode):
		if isinstance(path, str):
			path = path.encode()
		err = self._lib.mdb_env_open(env, path, flags, mode)
		if err != 0:
			raise Error(self.strerror(err))
	
	def env_copy(self, env, path):
		err = self._lib.mdb_env_copy(env, path)
		if err != 0:
			raise Error(self.strerror(err))

	def env_copyfd(self, env, fd):
		err = self._lib.mdb_env_copyfd(env, fd)
		if err != 0:
			raise Error(self.strerror(err))
	
	def env_stat(self, env):
		res = Stat()
		err = self._lib.mdb_env_stat(env, ctypes.pointer(res))
		if err != 0:
			raise Error(self.strerror(err))
		return res
	
	def env_info(self, env):
		res = EnvInfo()
		err = self._lib.mdb_env_info(env, ctypes.pointer(res))
		if err != 0:
			raise Error(self.strerror(err))
		return res
	
	def env_sync(self, env, force):
		err = self._lib.mdb_env_sync(env, force)
		if err != 0:
			raise Error(self.strerror(err))

	def env_close(self, env):
		self._lib.mdb_env_close(env)

	def env_set_flags(self, env, flags, onoff):
		err = self._lib.mdb_env_set_flags(env, flags, onoff)
		if err != 0:
			raise Error(self.strerror(err))
	
	def env_get_flags(self, env):
		res = ctypes.c_uint()
		err = self._lib.mdb_env_get_flags(env, ctypes.pointer(res))
		if err != 0:
			raise Error(self.strerror(err))
		return res.value

	def env_get_path(self, env):
		res = ctypes.c_char_p()
		err = self._lib.mdb_env_get_path(env, ctypes.pointer(res))
		if err != 0:
			raise Error(self.strerror(err))
		return res.value.decode()

	def env_set_mapsize(self, env, size):
		err = self._lib.mdb_env_set_mapsize(env, size)
		if err != 0:
			raise Error(self.strerror(err))

	def env_set_maxreaders(self, env, readers):
		err = self._lib.mdb_env_set_maxreaders(env, readers)
		if err != 0:
			raise Error(self.strerror(err))
	
	def env_get_maxreaders(self, env):
		res = ctypes.c_uint()
		err = self._lib.mdb_env_get_maxreaders(env, ctypes.pointer(res))
		if err != 0:
			raise Error(self.strerror(err))
		return res.value

	def env_set_maxdbs(self, env, dbs):
		err = self._lib.mdb_env_set_maxdbs(env, dbs)
		if err != 0:
			raise Error(self.strerror(err))

	def env_get_maxkeysize(self, env):
		res = self._lib.mdb_env_get_maxkeysize(env)
		return res

	def txn_begin(self, env, parent, flags):
		res = ctypes.c_void_p()
		err = self._lib.mdb_txn_begin(env, parent, flags, ctypes.pointer(res))
		if err != 0:
			raise Error(self.strerror(err))
		return res

	def txn_env(self, txn):
		res = self._lib.mdb_txn_env(txn)
		return res

	def txn_commit(self, txn):
		err = self._lib.mdb_txn_commit(txn)
		if err != 0:
			raise Error(self.strerror(err))
	
	def txn_abort(self, txn):
		self._lib.mdb_txn_abort(txn)
	
	def txn_reset(self, txn):
		self._lib.mdb_txn_reset(txn)
	
	def txn_renew(self, txn):
		err = self._lib.mdb_txn_renew(txn)
		if err != 0:
			raise Error(self.strerror(err))
	
	def dbi_open(self, txn, name, flags):
		if isinstance(name, str):
			name = name.encode()
		res = ctypes.c_uint()
		err = self._lib.mdb_dbi_open(txn, name, flags, ctypes.pointer(res))
		if err != 0:
			raise Error(self.strerror(err))
		return res

	def stat(self, txn, dbi):
		res = Stat()
		err = self._lib.mdb_stat(txn, dbi, ctypes.pointer(res))
		if err != 0:
			raise Error(self.strerror(err))
		return res

	def dbi_flags(self, txn, dbi):
		res = ctypes.c_uint()
		err = self._lib.mdb_dbi_flags(txn, dbi, ctypes.pointer(res))
		if err != 0:
			raise Error(self.strerror(err))
		return res

	def dbi_close(self, txn, dbi):
		self._lib.mdb_dbi_close(txn, dbi)

	def drop(self, txn, dbi, delete=False):
		err = self._lib.mdb_drop(txn, dbi, delete)
		if err != 0:
			raise Error(self.strerror(err))

	def get(self, txn, dbi, key):
		res = Value()
		err = self._lib.mdb_get(txn, dbi, ctypes.pointer(key), ctypes.pointer(res))
		if err != 0:
			raise Error(self.strerror(err))
		return res

	def put(self, txn, dbi, key, value):
		err = self._lib.mdb_put(txn, dbi, ctypes.pointer(key), ctypes.pointer(value))
		if err != 0:
			raise Error(self.strerror(err))
	
	def delete(self, txn, dbi, key, value):
		err = self._lib.mdb_del(txn, dbi, ctypes.pointer(key), ctypes.pointer(value))
		if err != 0:
			raise Error(self.strerror(err))
	
class Environment(object):
	def __init__(self, lib):
		self._lib = lib
		self._handle = lib.env_create()

	def __del__(self):
		self.close()

	def open(self, path, flags, mode):
		self._lib.env_open(self._handle, path, flags, mode)

	def close(self):
		if self._handle is None:
			self._lib.env_close(self._handle)
		del self._handle

	def copy(self, path):
		self._lib.env_copy(self._handle, path)

	def copyfd(self, fd):
		self._lib.env_copyfd(self._handle, fd)
	
	def stat(self):
		return self._lib.env_stat(self._handle)

	def info(self):
		return self._lib.env_stat(self._handle)

	def sync(self, force=False):
		self._lib.env_sync(self._handle, force)

	def set_flags(self, flags, on_off=False):
		self._lib.env_set_flags(self._handle, flags, on_off)
	
	def get_flags(self):
		return self._lib.env_get_flags(self._handle)

	def get_path(self):
		return self._lib.env_get_path(self._handle)

	def set_mapsize(self, size):
		self._lib.env_set_mapsize(self._handle, size)
	
	def set_maxreaders(self, maxreaders):
		self._lib.env_set_maxreaders(self._handle, maxreaders)
	
	def get_maxreaders(self):
		return self._lib.env_get_maxreaders(self._handle)

	def set_maxdbs(self, maxdbs):
		self._lib.env_set_maxdbs(self._handle, maxdbs)

	def get_maxkeysize(self):
		return self._lib.env_get_maxkeysize(self._handle)

	def transaction(self, flags=0):
		return Transaction(self, flags=flags)

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
	def __init__(self, env, parent=None, flags=0, lib=None):
		if lib is None:
			lib = env._lib
		self._lib = lib
		self.env = env
		self._handle = self._lib.txn_begin(env._handle,
			parent._handle if parent is not None else None,
			flags)
	
	def __del__(self):
		self.abort()

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		if exc_type is None:
			self.commit()
		else:
			self.abort()

	def transaction(self, flags=0):
		return Transaction(self.env, self, flags)

	def commit(self):
		if self._handle is not None:
			self._lib.txn_commit(self._handle)
			self._handle = None
	
	def abort(self):
		if self._handle is not None:
			self._lib.txn_abort(self._handle)
			self._handle = None

	def reset(self):
		self._lib.txn_reset(self._handle)

	def renew(self):
		self._lib.txn_renew(self._handle)

class Database(object):
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

	def stat(self):
		return self._lib.stat(self.transaction._handle, self._handle)

	def flags(self):
		return self._lib.dbi_flags(self.transaction._handle, self._handle)

	def close(self):
		self._lib.dbi_close(self.transaction._handle, self._handle)

	def empty(self):
		self._lib.drop(self.transaction._handle, self._handle, False)

	def drop(self):
		self._lib.drop(self.transaction._handle, self._handle, True)

	def get(self, key):
		if not isinstance(key, Value):
			key = Value.from_object(key)
		return self._lib.get(self.transaction._handle, self._handle, key)

	def put(self, key, value):
		if not isinstance(key, Value):
			key = Value.from_object(key)
		if not isinstance(value, Value):
			value = Value.from_object(value)
		self._lib.put(self.transaction._handle, self._handle, key, value)

	def delete(self, key, value=None):
		if not isinstance(key, Value):
			key = Value.from_object(key)
		if value is not None and not isinstance(value, Value):
			value = Value.from_object(value)
		self._lib.delete(self.transaction._handle, self._handle, key, value)

	def __getitem__(self, key):
		return self.get(key)

	def __setitem__(self, key, value):
		self.put(key, value)

	def __delitem__(self, key):
		self.delete(key)

	@property
	def env(self):
		return self.transaction.env

if __name__ == "__main__":
	main()

