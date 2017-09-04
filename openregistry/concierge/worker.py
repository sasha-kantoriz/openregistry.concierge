# -*- coding: utf-8 -*-
import argparse
import logging
import os
import time
import yaml

from openprocurement_client.resources.lots import LotsClient
from openprocurement_client.resources.assets import AssetsClient
from openprocurement_client.exceptions import (
    Forbidden,
    InvalidResponse,
    RequestFailed,
    ResourceNotFound,
    UnprocessableEntity
)

from .utils import (
    broken_lot_resolved,
    continuous_changes_feed,
    log_broken_lot,
    log_patch_to_db,
    prepare_couchdb
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(
    logging.Formatter('[%(asctime)s %(levelname)-5.5s] %(message)s')
)
logger.addHandler(ch)

EXCEPTIONS = (InvalidResponse, Forbidden, RequestFailed, ResourceNotFound, UnprocessableEntity)


class BotWorker(object):
    def __init__(self, config):
        self.config = config
        self.sleep = self.config['time_to_sleep']
        self.lots_client = LotsClient(
            key=self.config['lots']['api']['token'],
            host_url=self.config['lots']['api']['url'],
            api_version=self.config['lots']['api']['version']
        )
        self.assets_client = AssetsClient(
            key=self.config['assets']['api']['token'],
            host_url=self.config['assets']['api']['url'],
            api_version=self.config['assets']['api']['version']
        )
        if self.config['db'].get('login', '') \
                and self.config['db'].get('password', ''):
            db_url = "http://{login}:{password}@{host}:{port}".format(
                **self.config['db']
            )
        else:
            db_url = "http://{host}:{port}".format(**self.config['db'])

        self.db = prepare_couchdb(db_url, self.config['db']['name'], logger)
        self.errors_doc = self.db.get('broken_lots')
        self.patch_log_doc = self.db.get('patch_requests')
        # self.broken_lots = {}

    def run(self):
        logger.info("Starting worker")
        while True:
            for lot in self.get_lot():

                # broken_lot = self.broken_lots.get(lot['id'], None)
                broken_lot = self.errors_doc['broken_lots'].get(lot['id'], None)
                if broken_lot:
                    if broken_lot['rev'] == lot['rev']:
                        continue
                    else:
                        # self.process_lots(self.broken_lots.pop(lot['id']))
                        errors_doc = broken_lot_resolved(self.db, logger, self.errors_doc, lot['id'])
                        self.process_lots(errors_doc['broken_lots'][lot['id']])
                else:
                    self.process_lots(lot)
            # logger.debug('Broken lots: {}'.format(self.broken_lots.keys()))
            time.sleep(self.sleep)

    def get_lot(self):
        try:
            return continuous_changes_feed(
                self.db, logger,
                filter_doc=self.config['db']['filter']
            )
        except Exception, e:
            logger.error("Error while getting lots: {}".format(e))

    def process_lots(self, lot):
        logger.info("Processing lot {}".format(lot['id']))
        if lot['status'] == 'verification':
            try:
                assets_available = self.check_assets(lot)
            except RequestFailed:
                logger.info(
                    "Due to fail in getting assets, lot {} is skipped".format(
                        lot['id']))
            else:
                if assets_available:
                    result, patched_assets = self.patch_assets(lot, 'verification', lot['id'])
                    if result is False:
                        logger.info("Assets {} will be repatched to 'pending'".format(patched_assets))
                        self.patch_assets({'assets': patched_assets}, 'pending')
                    else:
                        result, patched_assets = self.patch_assets(lot, 'active', lot['id'])
                        if result is False:
                            logger.info("Assets {} will be repatched to 'pending'".format(patched_assets))
                            self.patch_assets({'assets': patched_assets}, 'pending')
                        else:
                            result = self.patch_lot(lot, "active.salable")
                            if result is False:
                                # self.broken_lots[lot['id']] = lot
                                broken_lots = log_broken_lot(self.db, logger, self.errors_doc, lot)
                                logger.debug('Broken lots from db: {}'.format([
                                    lot for lot in broken_lots['broken_lots'].keys()
                                    if lot['resolved'] is False
                                ]))

                else:
                    self.patch_lot(lot, "pending")
        elif lot['status'] == 'dissolved':
            self.patch_assets(lot, 'pending')

    def check_assets(self, lot):
        for asset_id in lot['assets']:
            try:
                asset = self.assets_client.get_asset(asset_id).data
                logger.info('Successfully got asset {}'.format(asset_id))
            except ResourceNotFound as e:
                logger.error('Falied to get asset {0}: {1}'.format(asset_id,
                                                                   e.message))
                return False
            except RequestFailed as e:
                logger.error('Falied to get asset {0}: {1}'.format(asset_id,
                                                                   e.message))
                raise RequestFailed('Failed to get assets')
            if asset.status != 'pending':
                return False
        return True

    def patch_assets(self, lot, status, related_lot=None, retries=5, sleep=0.2):
        patched_assets = []
        for asset_id in lot['assets']:
            retries = retries
            sleep = sleep
            asset = {
                "data": {
                    "id": asset_id,
                    "status": status,
                    "relatedLot": related_lot
                }
            }
            while retries:
                try:
                    self.assets_client.patch_resource_item(asset_id, asset)
                except EXCEPTIONS as e:
                    retries -= 1
                    if retries:
                        time.sleep(sleep)
                        sleep *= 2
                        continue
                    else:
                        logger.error("Failed to patch asset {} to {} ({})".format(
                            asset_id, status, e.message))
                        return False, patched_assets
                else:
                    logger.info("Successfully patched asset {} to {}".format(
                        asset_id, status))
                    log_patch_to_db(self.db, logger, self.patch_log_doc,
                                    {"id": asset_id, "status": status, "resource_type": 'asset'})
                    patched_assets.append(asset_id)
                    break
        return True, patched_assets

    def patch_lot(self, lot, status, retries=5, sleep=0.2):
        while retries:
            try:
                self.lots_client.patch_resource_item(lot['id'], {"data": {"status": status}})
            except EXCEPTIONS as e:
                retries -= 1
                if retries:
                    time.sleep(sleep)
                    sleep *= 2
                    continue
                else:
                    logger.error("Failed to patch lot {} to {} ({})".format(lot['id'],
                                                                            status,
                                                                            e.message))
                    return False
            else:
                logger.info("Successfully patched lot {} to {}".format(lot['id'],
                                                                       status))
                log_patch_to_db(self.db, logger, self.patch_log_doc,
                                {"id": lot['id'], "status": status, "resource_type": 'lot'})
                return True


def main():
    parser = argparse.ArgumentParser(description='---- Labot Worker ----')
    parser.add_argument('config', type=str, help='Path to configuration file')
    params = parser.parse_args()
    if os.path.isfile(params.config):
        with open(params.config) as config_object:
            config = yaml.load(config_object.read())
        BotWorker(config).run()


if __name__ == "__main__":
    main()
