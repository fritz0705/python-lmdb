# coding: utf-8

import json
import random
import os

import bottle

import lmdb.lmdb as lmdb

class Application(bottle.Bottle):
	VERSION = "0.1"
	NAMES = {"Apple", "Pear", "Cucumber", "Pineapple"}
	
	def __init__(self, *args, **kwargs):
		if "environment" not in kwargs:
			self.environment = lmdb.Environment(lmdb.LibLMDB())
			self.environment.open(kwargs.get("path", "./"))
		else:
			self.environment = kwargs.pop("environment")

		if "name" not in kwargs:
			self.name = "!"
		else:
			self.name = kwargs.pop("name")

		bottle.Bottle.__init__(self, *args, **kwargs)

		self.route("/", "GET", self.handle_index)
		self.route("/_simple/<key>", "GET", self.handle_get)
		self.route("/_simple/<key>", "PUT", self.handle_set)
		self.route("/_simple/<key>", "DELETE", self.handle_delete)
		self.route("/_trans", "POST", self.handle_transaction)

	def handle_index(self):
		bottle.response.content_type = "application/json"
		return json.dumps({
			"version": self.VERSION,
			"name": self.name
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
		bottle.response.content_type = "application/octet-stream"
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
		txn_info = json.load(bottle.request.body)

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
						txn[key] = action
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
	if "LMDB_WEB_LIB" in os.environ:
		_lib = lmdb.LibLMDB(os.environ["LMDB_WEB_LIB"])
	else:
		_lib = lmdb.LibLMDB()
	_env = lmdb.Environment(_lib)
	if "LMDB_WEB_DBPATH" in os.environ:
		_env.open(os.environ["LMDB_WEB_DBPATH"])
	else:
		_env.open("./")
	application = Application(environment=_env)
except lmdb.Error:
	pass

