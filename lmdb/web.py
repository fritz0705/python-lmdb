# coding: utf-8

import json
import random
import os
import os.path

import bottle

import lmdb.lmdb as lmdb

class Application(bottle.Bottle):
	VERSION = "0.1"
	NAMES = {"Apple", "Pear", "Cucumber", "Pineapple"}
	
	def __init__(self, *args, **kwargs):
		environment = kwargs.pop("environment", None)
		name = kwargs.pop("name", None)

		if environment is None:
			self.environment = lmdb.Environment(lmdb.LibLMDB())
			self.environment.open(kwargs.pop("path", "./"))
		else:
			self.environment = environment

		if name is None:
			self.name = self.NAMES.pop()
		else:
			self.name = name

		catch_keys = kwargs.pop("catch_keys", False)

		bottle.Bottle.__init__(self, *args, **kwargs)

		self.route("/", "GET", self.handle_index)
		self.route("/_simple/<key:path>", "GET", self.handle_get)
		self.route("/_simple/<key:path>", "PUT", self.handle_set)
		self.route("/_simple/<key:path>", "DELETE", self.handle_delete)
		self.route("/_trans", "POST", self.handle_transaction)
		self.route("/_dump", "GET", self.handle_dump)
		if catch_keys is True:
			self.route("/<key>", "GET", self.handle_get)
			self.route("/<key>", "POST", self.handle_set)
			self.route("/<key>", "DELETE", self.handle_delete)

	def handle_index(self):
		bottle.response.content_type = "application/json"
		stat = self.environment.stat
		envinfo = self.environment.info
		return json.dumps({
			"version": self.VERSION,
			"name": self.name,
			"mdb": {
				"psize": stat.ms_psize,
				"depth": stat.ms_depth,
				"branch_pages": stat.ms_branch_pages,
				"leaf_pages": stat.ms_leaf_pages,
				"overflow_pages": stat.ms_overflow_pages,
				"entries": stat.ms_entries
			},
			"env": {
				"mapaddr": envinfo.me_mapaddr,
				"mapsize": envinfo.me_mapsize,
				"last_pgno": envinfo.me_last_pgno,
				"last_txnid": envinfo.me_last_txnid,
				"maxreaders": envinfo.me_maxreaders,
				"numreaders": envinfo.me_numreaders
			}
		})

	def handle_get(self, key):
		bottle.response.content_type = "application/json"
		try:
			with self.environment.begin(lmdb.MDB_RDONLY) as txn:
				data = txn[key]
		except lmdb.Error as err:
			bottle.response.status = 500
			return json.dumps(self._lmdb_error_to_json(err, key))
		except KeyError:
			bottle.response.status = 404
			return json.dumps(self._key_error_to_json(key))
		content_type = bottle.request.query.get("type", "binary")
		bottle.response.content_type = {
			"binary": "application/octet-stream",
			"plain": "text/plain",
			"xml": "application/xml",
			"xhtml": "application/xhtml+xml",
			"html": "text/html"
		}.get(content_type, content_type)
		return data

	def handle_set(self, key):
		bottle.response.content_type = "application/json"
		try:
			with self.environment.begin() as txn:
				txn[key] = bottle.request.body.read()
		except lmdb.Error as err:
			bottle.response.status = 500
			return json.dumps(self._lmdb_error_to_json(err, key))
		return json.dumps({
			"message": "success",
			"success": "set",
			"key": key
		})

	def handle_delete(self, key):
		bottle.response.content_type = "application/json"
		try:
			with self.environment.begin() as txn:
				del txn[key]
		except lmdb.Error as err:
			bottle.response.status = 500
			return json.dumps(self._lmdb_error_to_json(err, key))
		except KeyError:
			bottle.response.status = 404
			return json.dumps(self._key_error_to_json(key))
		return json.dumps({
			"message": "success",
			"success": "delete",
			"key": key
		})

	def handle_transaction(self):
		bottle.response.content_type = "application/json"
		txn_info = bottle.request.body.read().decode()
		txn_info = json.loads(txn_info)

		flags = 0 if txn_info.get("write", False) else lmdb.MDB_RDONLY
		steps = txn_info.get("steps", [])

		report = []
		txn = self.environment.begin()
		try:
			for step in steps:
				try:
					action = step["action"]
					key = step["key"]
				except KeyError:
					report.append([None, None, "invalid", True])
					txn.abort()
					break
				try:
					if action == "contains":
						if key not in txn:
							raise KeyError(key)
					elif action == "set":
						txn[key] = step.get("value", b"")
					elif action == "delete":
						del txn[key]
				except KeyError:
					if step.get("abort", True):
						report.append([action, key, "not_found", True])
						txn.abort()
						break
					else:
						report.append([action, key, "not_found", False])
				report.append([action, key, "success", False])
		except:
			txn.abort()
			raise
		else:
			txn.commit()

		return json.dumps({
			"message": "success",
			"success": "transaction",
			"report": report
		})

	def handle_dump(self):
		bottle.response.content_type = "application/json"
		steps = []
		with self.environment.begin() as txn:
			for key, value in txn.cursor():
				steps.append({"action": "set", "key": key.decode(), "value": value.decode()})
		return json.dumps({
			"write": True,
			"steps": steps
		})

	def _key_error_to_json(self, key):
		return {
			"message": "exception",
			"exception": "not_found",
			"key": key
		}

	def _lmdb_error_to_json(self, exc, key):
		return {
			"message": "exception",
			"exception": "lmdb_error",
			"errno": exc.code,
			"msg": exc.message,
			"key": key
		}

try:
	if "LMDB_WEB_LIB" in os.environ:
		_lib = lmdb.LibLMDB(os.environ["LMDB_WEB_LIB"])
	else:
		_lib = lmdb.LibLMDB()
	_env = lmdb.Environment(_lib)
	if "LMDB_WEB_DBPATH" in os.environ:
		if os.path.isfile(os.environ["LMDB_WEB_DBPATH"]):
			_env.open(os.environ["LMDB_WEB_DBPATH"], lmdb.MDB_NOSUBDIR)
		else:
			_env.open(os.environ["LMDB_WEB_DBPATH"])
	else:
		_env.open("./")
	application = Application(environment=_env)
except lmdb.Error:
	pass

