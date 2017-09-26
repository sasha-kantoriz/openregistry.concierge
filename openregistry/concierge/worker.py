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
    RequestFailed,
    ResourceNotFound,
    UnprocessableEntity
)

from .utils import (
    resolve_broken_lot,
    continuous_changes_feed,
    log_broken_lot,
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

EXCEPTIONS = (Forbidden, RequestFailed, ResourceNotFound, UnprocessableEntity)


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

        self.db = prepare_couchdb(db_url, self.config['db']['name'], logger, self.config['errors_doc'])
        self.errors_doc = self.db.get(self.config['errors_doc'])
        self.patch_log_doc = self.db.get('patch_requests')

    def run(self):
        logger.info("Starting worker")
        while True:
            for lot in self.get_lot():
                broken_lot = self.errors_doc.get(lot['id'], None)
                if broken_lot:
                    if broken_lot['rev'] == lot['rev']:
                        continue
                    else:
                        errors_doc = resolve_broken_lot(self.db, logger, self.errors_doc, lot)
                        self.process_lots(errors_doc[lot['id']])
                else:
                    self.process_lots(lot)
            time.sleep(self.sleep)

    def get_lot(self):
        logger.info('Getting Lots')
        return continuous_changes_feed(
            self.db, logger,
            filter_doc=self.config['db']['filter']
        )

    def process_lots(self, lot):
        lot_available = self.check_lot(lot)
        if not lot_available:
            logger.info("Skipping lot {}".format(lot['id']))
            return
        logger.info("Processing lot {}".format(lot['id']))
        if lot['status'] == 'verification':
            try:
                assets_available = self.check_assets(lot)
            except RequestFailed:
                logger.info("Due to fail in getting assets, lot {} is skipped".format(lot['id']))
            else:
                if assets_available:
                    result, patched_assets = self.patch_assets(lot, 'verification', lot['id'])
                    if result is False:
                        if patched_assets:
                            logger.info("Assets {} will be repatched to 'pending'".format(patched_assets))
                            result, _ = self.patch_assets({'assets': patched_assets}, 'pending')
                            if result is False:
                                log_broken_lot(self.db, logger, self.errors_doc, lot, 'patching assets to verification')
                    else:
                        result, _ = self.patch_assets(lot, 'active', lot['id'])
                        if result is False:
                            logger.info("Assets {} will be repatched to 'pending'".format(lot['assets']))
                            result, _ = self.patch_assets(lot, 'pending')
                            if result is False:
                                log_broken_lot(self.db, logger, self.errors_doc, lot, 'patching assets to active')
                        else:
                            result = self.patch_lot(lot, "active.salable")
                            if result is False:
                                log_broken_lot(self.db, logger, self.errors_doc, lot, 'patching lot to active.salable')
                else:
                    self.patch_lot(lot, "pending")
        elif lot['status'] == 'pending.dissolution':
            self.patch_assets(lot, 'pending')
            self.patch_lot(lot, 'dissolved')

    def check_lot(self, lot):
        try:
            lot = self.lots_client.get_lot(lot['id']).data
            logger.info('Successfully got lot {}'.format(lot['id']))
        except ResourceNotFound as e:
            logger.error('Falied to get lot {0}: {1}'.format(lot['id'], e.message))
            return False
        except RequestFailed as e:
            logger.error('Falied to get lot {0}. Status code: {1}'.format(lot['id'], e.status_code))
            return False
        if lot.status != 'verification' and lot.status != 'dissolved':
            logger.warning("Lot {0} can not be processed in current status ('{1}')".format(lot.id, lot.status))
            return False
        return True

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
                logger.error('Falied to get asset {0}. Status code: {1}'.format(asset_id, e.status_code))
                raise RequestFailed('Failed to get assets')
            if asset.status != 'pending':
                return False
        return True

    def patch_assets(self, lot, status, related_lot=None):
        patched_assets = []
        for asset_id in lot['assets']:
            asset = {"data": {"status": status, "relatedLot": related_lot}}
            try:
                self.assets_client.patch_asset(asset_id, asset)
            except EXCEPTIONS as e:
                message = e.message
                if e.status_code >= 500:
                    message = 'Server error: {}'.format(e.status_code)
                logger.error("Failed to patch asset {} to {} ({})".format(asset_id, status, message))
                return False, patched_assets
            else:
                logger.info("Successfully patched asset {} to {}".format(asset_id, status))
                patched_assets.append(asset_id)
        return True, patched_assets

    def patch_lot(self, lot, status):
        try:
            self.lots_client.patch_lot(lot['id'], {"data": {"status": status}})
        except EXCEPTIONS as e:
            message = e.message
            if e.status_code >= 500:
                message = 'Server error: {}'.format(e.status_code)
            logger.error("Failed to patch lot {} to {} ({})".format(lot['id'], status, message))
            return False
        else:
            logger.info("Successfully patched lot {} to {}".format(lot['id'], status))
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
