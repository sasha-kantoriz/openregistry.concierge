# -*- coding: utf-8 -*-
from couchdb import Server, Session
from socket import error

from .design import sync_design


class ConfigError(Exception):
    pass


def prepare_couchdb(couch_url, db_name, logger):
    server = Server(couch_url, session=Session(retry_delays=range(10)))
    try:
        if db_name not in server:
            db = server.create(db_name)
        else:
            db = server[db_name]
    except error as e:
        logger.error('Database error: {}'.format(e.message))
        raise ConfigError(e.strerror)
    sync_design(db)
    return db