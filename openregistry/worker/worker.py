# -*- coding: utf-8 -*-
import argparse
import logging
import os
import time
import yaml

from couchdb.http import RETRYABLE_ERRORS

from openprocurement_client.registry_client import RegistryClient
from openprocurement_client.exceptions import InvalidResponse, Forbidden, RequestFailed

from .utils import prepare_couchdb

logger = logging.getLogger(__name__)

class BotWorker(object):
    def __init__(self, config, client):
        self.config = config
        self.sleep = self.config['time_to_sleep']
        self.lots_client = client(
            resource="lots",
            key=self.config['lots']['api']['token'],
            host_url=self.config['lots']['api']['url'],
            api_version=self.config['lots']['api']['version']
        )
        self.assets_client = client(
            resource="assets",
            key=self.config['assets']['api']['token'],
            host_url=self.config['assets']['api']['url'],
            api_version=self.config['assets']['api']['version']
        )
        self.db = prepare_couchdb(self.config['db']['url'], self.config['db']['name'], logger)

    def run(self):
        logger.info("Starting worker")
        while True:
            for lot in self.get_lot():
                self.process_lot(lot)
            time.sleep(self.sleep)

    def get_lot(self):
        view = self.config['db']['view']
        logger.info("Getting lots")
        try:
            for row in self.db.iterview(view, batch=10):
                yield row.value
        except Exception, e:
            logger.error("Error while getting lots: {}".format(e))

    def process_lots(self, lot):
        logger.info("Processing lot {}".format(lot['id']))
        if lot['status'] == 'waiting':
            try:
                assets_available = self.check_assets(lot)
            except RequestFailed:
                logger.info("Due to fail in getting assets, lot {} is skipped".format(lot['id']))
            if assets_available:
                self.patch_lot(lot, "active.pending")
                self.patch_assets(lot, 'active', lot['id'])
            else:
                self.patch_lot(lot, "invalid")
        elif lot['status'] == 'dissolved':
            self.patch_assets(lot, 'pending')

    def check_assets(self, lot):
        for asset_id in lot['assets']:
            try:
                asset = self.assets_client.get_asset(asset_id).data
                logger.info('Successfully got asset {}'.format(asset_id))
            except RequestFailed as e:
                logger.error('Falied to get asset: {}'.format(e.message))
                raise RequestFailed('Failed to get assets')
            if asset.status != 'pending':
                return False
        return True

    def patch_assets(self, lot, status, related_lot=None):
        for asset_id in lot['assets']:
            asset = {"data": {"id": asset_id, "status": status, "relatedLot": related_lot}}
            try:
                self.assets_client.patch_asset(asset)
            except (InvalidResponse, Forbidden, RequestFailed) as e:
                logger.error("Failed to patch asset {} to {} ({})".format(asset_id, status, e))
                return False
            else:
                logger.info("Successfully patched asset {} to {}".format(asset_id, status))
        return True

    def patch_lot(self, lot, status):
        try:
            self.lots_client.patch_lot({"status": status})
        except (InvalidResponse, Forbidden, RequestFailed):
            logger.error("Failed to patch lot {} to {}".format(lot['id'], status))
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
        BotWorker(config, RegistryClient).run()

if __name__ == "__main__":
    main()
