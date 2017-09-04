# -*- coding: utf-8 -*-
import os

from couchdb import Server, Session
from datetime import datetime
from socket import error
from pytz import timezone

from .design import sync_design

CONTINUOUS_CHANGES_FEED_FLAG = True
TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')


class ConfigError(Exception):
    pass


def get_now():
    return datetime.now(TZ)


def prepare_couchdb(couch_url, db_name, logger):
    server = Server(couch_url, session=Session(retry_delays=range(10)))
    try:
        if db_name not in server:
            db = server.create(db_name)
        else:
            db = server[db_name]

        broken_lots = db.get('broken_lots', None)
        if broken_lots is None:
            db['broken_lots'] = {'broken_lots': {}}

        patch_requests = db.get('patch_requests', None)
        if patch_requests is None:
            db['patch_requests'] = {'patch_requests': {}}

    except error as e:
        logger.error('Database error: {}'.format(e.message))
        raise ConfigError(e.strerror)
    sync_design(db)
    return db


def continuous_changes_feed(db, logger, limit=100, filter_doc='lots/status'):

    last_seq_id = 0
    while CONTINUOUS_CHANGES_FEED_FLAG:
        logger.info('Getting Lots')
        data = db.changes(include_docs=True, since=last_seq_id, limit=limit,
                          filter=filter_doc)
        last_seq_id = data['last_seq']
        if len(data['results']) != 0:
            for row in data['results']:
                item = {
                    'id': row['doc']['_id'],
                    'rev': row['doc']['_rev'],
                    'status': row['doc']['status'],
                    'assets': row['doc']['assets'],
                    'lotID': row['doc']['lotID']
                }
                yield item
        else:
            break


def log_patch_to_db(db, logger, doc, resource):
    try:
        doc['patch_requests'][get_now().isoformat()] = resource
        db.save(doc)
    except error as e:
        logger.error('Database error: {}'.format(e.message))
        raise ConfigError(e.strerror)
    else:
        return doc


def log_broken_lot(db, logger, doc, lot):
    lot['resolved'] = False
    try:
        doc['broken_lots'][lot['id']] = lot
        db.save(doc)
    except error as e:
        logger.error('Database error: {}'.format(e.message))
        raise ConfigError(e.strerror)
    else:
        return doc


def broken_lot_resolved(db, logger, doc, lot_id):
    try:
        doc['broken_lots'][lot_id]['resolved'] = True
        db.save(doc)
    except error as e:
        logger.error('Database error: {}'.format(e.message))
        raise ConfigError(e.strerror)
    else:
        return doc
