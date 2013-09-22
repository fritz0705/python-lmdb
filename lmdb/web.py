# coding: utf-8

import json
import random
import os
import os.path

import bottle
import werkzeug.http

import lmdb.lmdb as lmdb

class Application(bottle.Bottle):
	VERSION = "0.1"
	NAMES = {"Apple", "Pear", "Cucumber", "Pineapple"}

	request = bottle.request
	response = bottle.response
	
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

		bottle.Bottle.__init__(self, *args, **kwargs)

		self.route("/", "GET", self.handle_index)
		self.route("/", "TRANSACTION", self.handle_transaction)

		self.route("/<key:path>", "GET", self.handle_get)
		self.route("/<key:path>", "PUT", self.handle_set)
		self.route("/<key:path>", "DELETE", self.handle_delete)

	def _pick_type(self, default="text/plain"):
		if "Accept" in self.request.headers:
			accepted = werkzeug.http.parse_accept_header(self.request.headers["Accept"])
			return accepted.best_match([
				"application/json",
				"text/html",
				"application/xml+xhtml",
				"text/plain",
				"application/octet-stream"
			], accepted.best)
		return default

	def handle_index(self):
		self.response.content_type = "application/json"
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
		self.response.content_type = "application/json"
		try:
			with self.environment.begin(lmdb.MDB_RDONLY) as txn:
				data = txn[key]
		except lmdb.Error as err:
			self.response.status = 500
			return json.dumps(self._lmdb_error_to_json(err, key))
		except KeyError:
			self.response.status = 404
			return json.dumps(self._key_error_to_json(key))
		self.response.content_type = self._pick_type()
		return data

	def handle_set(self, key):
		self.response.content_type = "application/json"
		try:
			with self.environment.begin() as txn:
				txn[key] = self.request.body.read()
		except lmdb.Error as err:
			self.response.status = 500
			return json.dumps(self._lmdb_error_to_json(err, key))
		return json.dumps({
			"message": "success",
			"success": "set",
			"key": key
		})

	def handle_delete(self, key):
		self.response.content_type = "application/json"
		try:
			with self.environment.begin() as txn:
				del txn[key]
		except lmdb.Error as err:
			self.response.status = 500
			return json.dumps(self._lmdb_error_to_json(err, key))
		except KeyError:
			self.response.status = 404
			return json.dumps(self._key_error_to_json(key))
		return json.dumps({
			"message": "success",
			"success": "delete",
			"key": key
		})

	def handle_transaction(self):
		self.response.content_type = "application/json"
		txn_info = self.request.body.read().decode()
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
	_env = lmdb.Environment(lmdb.lib)
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

