# -*- coding: utf-8 -*-

"""Provides utilities for connecting to SQL databases and for logging."""

from configparser import ConfigParser
import logging
import os
import sqlalchemy
from utilities.decorators import error_trap


def _get_config(filename, section):
    parser = ConfigParser()  # create a parser
    parser.read(filename)    # read config file

    # get section
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('"{}" section not found in {}'.format(section,
                                                              filename))
    return db


@error_trap
def _connect(section, filename):
    # read connection parameters and build connection url
    params = _get_config(filename, section)
    dialect = params['dialect']
    if params.get('connector'):
        dialect += '+' + params['connector']
    db_url = '{}://{}:{}@{}'.format(dialect, params['user'],
                                    params['password'], params['host'])
    if params.get('port'):
        db_url += ':{}'.format(params['port'])
    db_url += '/{}'.format(params['dbname'])

    # connect to the SQL server
    print('Connecting to the database...')
    con = sqlalchemy.create_engine(db_url).connect()
    if params.get('search_path'):
        con.execute('SET search_path TO ' + params['search_path'])

    # display the database name, and server dialect and version
    db_version = con.execute('SELECT version()').fetchall()[0][0]
    print('{} -- {} VERSION {}'.format(params['dbname'].upper(),
                                       params['dialect'].upper(),
                                       db_version))
    return con


def connect(section,
            filename=os.path.expanduser('~/projects/utilities/database.ini')):
    """Connect to the SQL database server and return connectable."""
    con, err = _connect(section, filename)
    if err:
        print(err)
        return None
    else:
        return con


class Logger(object):
    def __init__(self, name=__name__, log_path='.', log_file='out.log',
                 logger_level=logging.DEBUG, file_level=logging.DEBUG,
                 stream_level=logging.ERROR, date_format='%Y-%m-%d %H:%M:%S',
                 log_format='%(asctime)s - %(name)s - ' +
                            '%(levelname)s - %(message)s'):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logger_level)
        fh = logging.FileHandler(os.path.join(log_path, name + '-' + log_file))
        fh.setLevel(file_level)
        ch = logging.StreamHandler()
        ch.setLevel(stream_level)
        formatter = logging.Formatter(log_format, datefmt=date_format)
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.addHandler(fh)

    def critical(self, message):
        return self.logger.critical(message)

    def debug(self, message):
        return self.logger.debug(message)

    def error(self, message):
        return self.logger.error(message)

    def exception(self, message):
        return self.logger.exception(message)

    def info(self, message):
        return self.logger.info(message)

    def warning(self, message):
        return self.logger.warning(message)
