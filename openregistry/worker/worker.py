# -*- coding: utf-8 -*-
import os
import time
import yaml

from couchdb import Database, Session

from openprocurement_client.registry_client import RegistryClient

PWD = os.path.dirname(os.path.realpath(__file__))


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
        return ({"data": {
            'id': lot.id,
            'assets': lot.value['assets'],
            'status': lot.value['status']}}for lot in self.db.view(view) if lot.value['status'] == 'waiting')

    def check_lot_assets(self, assets):
        for asset in assets:
            status = self.assets_client.get_asset(asset).data.status
            if status != 'pending':
                return False
        else:
            return True

    def process_lots(self, lots):
        for lot in lots:
            result = self.check_lot_assets(lot['data']['assets'])
            self.make_patch(lot, result)

    def make_patch(self, lot, result):
        if result:
            lot['data']['status'] = "active.pending"
            self.lots_client.patch_lot(lot)

            for asset_id in lot['data']['assets']:
                asset = {"data": {"id": asset_id, "status": "active"}}
                self.assets_client.patch_asset(asset)
        else:
            lot['data']['status'] = "invalid"
            self.lots_client.patch_lot(lot)


def main(bot):
    bot.process_lots(bot.get_lots(bot.bot_conf['LOTS_DB']['view']))


def run():
    config_path = os.path.join(PWD, "bot_conf.yaml")
    config = yaml.load(open(config_path))
    bot = BotWorker(config)
    while True:
        main(bot)
        time.sleep(bot.sleep)

if __name__ == "__main__":
    run()
