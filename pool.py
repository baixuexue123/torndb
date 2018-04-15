import os
import time
import logging
from collections import OrderedDict
from contextlib import contextmanager
from inspect import isclass

import pymysql
from sqlalchemy.engine.result import ResultProxy


class Pool(object):

    def __init__(self):
        pass

    def dispose(self):
        pass
