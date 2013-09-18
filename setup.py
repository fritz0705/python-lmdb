#!/usr/bin/env python3

try:
	with open("README.md") as fh:
		long_description = fh.read()
except FileNotFoundError:
	long_description = ""
	pass

import setuptools

setuptools.setup(
	name="python-lmdb",
	version="1.0.0b1",
	packages=[
		"lmdb"
	],
	author="Fritz Grimpen",
	author_email="fritz@grimpen.net",
	url="http://github.com/fritz0705/python-lmdb.git",
	license="http://opensource.org/licenses/MIT",
	description="simple lmdb bindings written using ctypes",
	classifiers=[
		"Development Status :: 4 - Beta",
		"Operating System :: POSIX",
		"Programming Language :: Python :: 3.3",
		"Topic :: Database :: Database Engines/Servers"
	],
	long_description=long_description
)
