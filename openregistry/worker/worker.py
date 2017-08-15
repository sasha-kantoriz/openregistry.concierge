# -*- coding: utf-8 -*-
import argparse
import logging
import os
import time
import yaml

from couchdb import Database, Session
from couchdb.http import RETRYABLE_ERRORS

from openprocurement_client.registry_client import RegistryClient
from openprocurement_client.exceptions import InvalidResponse, Forbidden, RequestFailed

PWD = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('[%(asctime)s %(levelname)-5.5s] %(message)s'))
logger.addHandler(ch)


class BotWorker(object):
    def __init__(self, config, client):
        self.config = config
        self.sleep = self.config['TIME_TO_SLEEP']
        self.lots_client = client(
            key=self.config['LOTS_API_TOKEN'],
            host_url=self.config["API_URL"],
            api_version=self.config["API_VERSION"]
        )
        self.assets_client = client(
            resource="assets",
            key=self.config['ASSETS_API_TOKEN'],
            host_url=self.config["API_URL"],
            api_version=self.config["API_VERSION"]
        )
        self.db = Database("http://{login}:{password}@{host}:{port}/{db}".format(**self.config['LOTS_DB']),
                           session=Session(retry_delays=range(10)))

    def get_lots(self, view):
        logger.info("Getting lots")
        try:
            return ({"data": {
                'id': lot.id,
                'assets': lot.value['assets'],
                'status': lot.value['status']}}for lot in self.db.view(view)
                    if lot.value['status'] in ['waiting', 'dissolved'])
        except Exception, e:
            ecode = e.args[0]
            if ecode in RETRYABLE_ERRORS:
                logger.error("Error while getting lots: {}".format(e))

    def check_lot_assets(self, lot):
        assets = lot['data']['assets']
        lot_status = lot['data']['status']

        for asset_id in assets:
            try:
                asset = self.assets_client.get_asset(asset_id).data
            except RequestFailed as e:
                logger.error('Falied to get asset: {}'.format(e.message))
                raise RequestFailed('Failed to get assets')
            else:
                logger.info('Successfully got asset {}'.format(asset_id))
                related_lot = asset.get('relatedLot', '')
                if asset.status != 'pending':
                    if lot_status == "waiting" or lot_status == "dissolved" and related_lot == lot['data']['id']:
                        return False
        return True

    def patch_assets(self, lot, status, related_lot=None):
        for asset_id in lot['data']['assets']:
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

        lot['data']['status'] = status
        try:
            self.lots_client.patch_lot(lot)
        except (InvalidResponse, Forbidden, RequestFailed):
            logger.error("Failed to patch lot {} to {}".format(lot['data']['id'], status))
            return False
        else:
            logger.info("Successfully patched lot {} to {}".format(lot['data']['id'], status))
            return True

    def process_lots(self, lots):
        if not lots:
            logger.info("Could not get any lots")
            return
        logger.info("Got lots")
        for lot in lots:
            logger.info("Processing lot {}".format(lot['data']['id']))
            try:
                assets_available = self.check_lot_assets(lot)
            except RequestFailed:
                logger.info("Due to fail in getting assets, lot {} is skipped".format(lot['data']['id']))
            else:
                self.make_patch(lot, assets_available)
        logger.info("Processed all lots")

    def make_patch(self, lot, assets_available):
        if lot['data']['status'] == 'waiting':
            if assets_available:
                self.patch_lot(lot, "active.pending")
                self.patch_assets(lot, 'active', lot['data']['id'])
            else:
                self.patch_lot(lot, "invalid")
        elif lot['data']['status'] == 'dissolved' and not assets_available:
            self.patch_assets(lot, 'pending')

    def run(self):
        logger.info("Starting worker")
        while True:
            self.process_lots(self.get_lots(self.config['LOTS_DB']['view']))
            time.sleep(self.sleep)


def main():
    config_path = os.path.join(PWD, "bot_conf.yaml")
    config = yaml.load(open(config_path))
    BotWorker(config, RegistryClient).run()
    # parser = argparse.ArgumentParser(description='---- Labot Worker ----')
    # parser.add_argument('config', type=str, help='Path to configuration file')
    # params = parser.parse_args()
    # if os.path.isfile(params.config):
    #     with open(params.config) as config_object:
    #         config = yaml.load(config_object.read())
    #     BotWorker(config, RegistryClient).run()

if __name__ == "__main__":
    main()
