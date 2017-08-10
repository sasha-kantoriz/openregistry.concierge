# -*- coding: utf-8 -*-
import logging
import os
import time
import yaml

from couchdb import Database, Session

from openprocurement_client.registry_client import RegistryClient
from openprocurement_client.exceptions import InvalidResponse, Forbidden

PWD = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('[%(asctime)s %(levelname)-5.5s] %(message)s'))
logger.addHandler(ch)


class BotWorker(object):
    def __init__(self, bot_conf):
        self.bot_conf = bot_conf
        self.sleep = self.bot_conf['TIME_TO_SLEEP']
        self.lots_client = RegistryClient(
            key=self.bot_conf['LOTS_API_TOKEN'],
            host_url=self.bot_conf["API_URL"],
            api_version=self.bot_conf["API_VERSION"]
        )
        self.assets_client = RegistryClient(
            resource="assets",
            key=self.bot_conf['ASSETS_API_TOKEN'],
            host_url=self.bot_conf["API_URL"],
            api_version=self.bot_conf["API_VERSION"]
        )
        self.db = Database("http://{login}:{password}@{host}:{port}/{db}".format(**self.bot_conf['LOTS_DB']),
                           session=Session(retry_delays=range(10)))

    def get_lots(self, view):
        logger.info("Getting lots")
        return ({"data": {
            'id': lot.id,
            'assets': lot.value['assets'],
            'status': lot.value['status']}}for lot in self.db.view(view)
                if lot.value['status'] in ['waiting', 'dissolved'])

    def check_lot_assets(self, lot):
        assets = lot['data']['assets']
        lot_status = lot['data']['status']

        for asset in assets:
            asset = self.assets_client.get_asset(asset).data
            if asset.status != 'pending':
                # if lot_status == "waiting" or asset.relatedLot == lot['data']['id']: TODO: wait for proper asset serialization
                if lot_status == "waiting" or lot_status == "dissolved":
                    return False
        return True

    def patch_assets(self, lot, status):

        for asset_id in lot['data']['assets']:
            asset = {"data": {"id": asset_id, "status": status, "relatedLot": lot['data']['id']}}
            try:
                self.assets_client.patch_asset(asset)
            except (InvalidResponse, Forbidden):
                logger.error("Failed to patch asset {} to {}".format(asset_id, status))
            else:
                logger.info("Successfully patched asset {} to {}".format(asset_id, status))

    def patch_lot(self, lot, status):

        lot['data']['status'] = status
        try:
            self.lots_client.patch_lot(lot)
        except (InvalidResponse, Forbidden):
            logger.error("Failed to patch lot {} to {}".format(lot['data']['id'], status))
        else:
            logger.info("Successfully patched lot {} to {}".format(lot['data']['id'], status))

    def process_lots(self, lots):
        logger.info("Get lots")
        for lot in lots:
            logger.info("Processing lot {}".format(lot['data']['id']))
            assets_available = self.check_lot_assets(lot)
            self.make_patch(lot, assets_available)
        logger.info("Processed all lots")

    def make_patch(self, lot, assets_available):
        # if assets_available and lot['data']['status'] == 'waiting':
        #     self.patch_lot(lot, "active.pending")
        #     self.patch_assets(lot, 'active')
        # else:
        #     if lot['data']['status'] == 'waiting':
        #         self.patch_lot(lot, "invalid")
        #     elif lot['data']['status'] == 'dissolved':
        #         self.patch_assets(lot, 'pending')
        if lot['data']['status'] == 'waiting':
            if assets_available:
                self.patch_lot(lot, "active.pending")
                self.patch_assets(lot, 'active')
            else:
                self.patch_lot(lot, "invalid")
        elif lot['data']['status'] == 'dissolved' and not assets_available:
            self.patch_assets(lot, 'pending')

    def run(self):
        logger.info("Starting worker")
        while True:
            self.process_lots(self.get_lots(self.bot_conf['LOTS_DB']['view']))
            time.sleep(self.sleep)


def main():

    config_path = os.path.join(PWD, "../bot_conf.yaml")
    config = yaml.load(open(config_path))
    BotWorker(config).run()

if __name__ == "__main__":
    main()
