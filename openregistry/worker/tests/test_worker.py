# -*- coding: utf-8 -*-
import errno
import os
import pytest
import time

from couchdb import Database
from json import load
from munch import munchify

from openprocurement_client.exceptions import RequestFailed, InvalidResponse, Forbidden

ROOT = os.path.dirname(__file__) + '/data/'


def test_get_lots(bot, logger, mocker):
    mock_database_view = mocker.patch.object(bot.db, 'view', autospec=True)
    with open(ROOT + 'lots_couchdb_view.json') as lots:
        mock_database_view.return_value = munchify(load(lots))['rows']
    lots = bot.get_lots(bot.bot_conf['LOTS_DB']['view'])
    assert lots.next() == {'data':
        {
            'status': 'dissolved',
            'id': 'fd122ba678174a19affcb3a0edc96e0e',
            'assets': ["a107b69548b54eb293ad82b36c6936a1"]
        }
    }
    assert lots.next() == {'data':
        {
            'status': 'dissolved',
            'id': 'c5cd5530af8547439d02dcde60750567',
            'assets': [
                "137738e77e1c4a968a2f1c4226639854",
                "d1d4c1985ec943d2b1a3db67cc577c62"
            ]
        }
    }
    assert lots.next() == {'data':
        {
            'status': 'waiting',
            'id': 'dd36079db10f4b77b8dd77ca64299e95',
            'assets': [
                "b30136280e2a4d8f8c3572d52b155741",
                "49faa46e7b344234aa3bbe75866b55a8",
                "d70982e5d32647d0847e9f2f9b26439c"
            ]
        }
    }
    with pytest.raises(StopIteration):
        lots.next()

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Getting lots"


def test_get_lots_failed(bot, logger, mocker):
    mock_database_view = mocker.patch.object(Database, 'view', autospec=True)
    mock_database_view.side_effect = Exception(errno.EPIPE, 'retryable error message')
    bot.get_lots(bot.bot_conf['LOTS_DB']['view'])

    log_strings = logger.log_capture_string.getvalue().split('\n')

    assert log_strings[0] == "Getting lots"
    assert log_strings[1] == "Error while getting lots: (32, 'retryable error message')"


def test_check_lot_assets_available(bot, logger, mock_client):
    lot = {'data':
        {
            'status': 'waiting',
            'id': 'dd36079db10f4b77b8dd77ca64299e95',
            'assets': [
                "b30136280e2a4d8f8c3572d52b155741",  # status: pending
                "49faa46e7b344234aa3bbe75866b55a8",  # status: pending
                "d70982e5d32647d0847e9f2f9b26439c"  # status: pending
            ]
        }
    }

    with open(ROOT + 'assets.json') as assets_data:
        assets = munchify(load(assets_data))

    bot.assets_client.get_asset.side_effect = [
        assets['b30136280e2a4d8f8c3572d52b155741'],
        assets['49faa46e7b344234aa3bbe75866b55a8'],
        assets['d70982e5d32647d0847e9f2f9b26439c']
    ]

    result = bot.check_lot_assets(lot)
    assert result is True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Successfully got asset b30136280e2a4d8f8c3572d52b155741"
    assert log_strings[1] == "Successfully got asset 49faa46e7b344234aa3bbe75866b55a8"
    assert log_strings[2] == "Successfully got asset d70982e5d32647d0847e9f2f9b26439c"


def test_check_lot_assets_not_available(bot, logger, mock_client):
    lot = {'data':
        {
            'status': 'waiting',
            'id': 'dd36079db10f4b77b8dd77ca64299e95',
            'assets': [
                "137738e77e1c4a968a2f1c4226639854",  # status: pending
                "a7d2cbaf3e9242cea92daecb61a565dc"  # status: active
            ]
        }
    }

    with open(ROOT + 'assets.json') as assets_data:
        assets = munchify(load(assets_data))

    bot.assets_client.get_asset.side_effect = [
        assets['137738e77e1c4a968a2f1c4226639854'],
        assets['a7d2cbaf3e9242cea92daecb61a565dc']
    ]

    result = bot.check_lot_assets(lot)
    assert result is False

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Successfully got asset 137738e77e1c4a968a2f1c4226639854"
    assert log_strings[1] == "Successfully got asset a7d2cbaf3e9242cea92daecb61a565dc"


def test_check_lot_asset_request_failed(bot, logger, mock_client):
    lot = {'data':
        {
            'status': 'waiting',
            'id': 'dd36079db10f4b77b8dd77ca64299e95',
            'assets': [
                "137738e77e1c4a968a2f1c4226639854",  # status: pending
            ]
        }
    }

    with open(ROOT + 'assets.json') as assets_data:
        assets = munchify(load(assets_data))

    bot.assets_client.get_asset.side_effect = [
        RequestFailed()
    ]

    with pytest.raises(RequestFailed):
        bot.check_lot_assets(lot)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Falied to get asset: Not described error yet."


def test_patch_assets_success(bot, logger, mock_client):
    lot = {'data':
        {
            'status': 'waiting',
            'id': 'dd36079db10f4b77b8dd77ca64299e95',
            'assets': [
                "137738e77e1c4a968a2f1c4226639854",  # status: pending
            ]
        }
    }

    with open(ROOT + 'assets.json') as assets_data:
        assets = munchify(load(assets_data))

    asset = assets['137738e77e1c4a968a2f1c4226639854']

    bot.assets_client.patch_asset.return_value = str(asset)
    result = bot.patch_assets(lot, 'pending', lot['data']['id'])
    assert result is True

    asset.data.status = 'active'

    bot.assets_client.patch_asset.return_value = str(asset)
    result = bot.patch_assets(lot, 'active', lot['data']['id'])
    assert result is True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Successfully patched asset 137738e77e1c4a968a2f1c4226639854 to pending"
    assert log_strings[1] == "Successfully patched asset 137738e77e1c4a968a2f1c4226639854 to active"


def test_patch_assets_failed(bot, logger, mock_client):
    lot = {'data':
        {
            'status': 'waiting',
            'id': 'dd36079db10f4b77b8dd77ca64299e95',
            'assets': [
                "137738e77e1c4a968a2f1c4226639854",  # status: pending
                "b30136280e2a4d8f8c3572d52b155741",  # status: pending
            ]
        }
    }

    with open(ROOT + 'assets.json') as assets_data:
        assets = munchify(load(assets_data))

    asset = assets['137738e77e1c4a968a2f1c4226639854']
    asset.data.status = 'active'

    bot.assets_client.patch_asset.side_effect = [
        str(asset),
        RequestFailed()
    ]
    result = bot.patch_assets(lot, 'active', lot['data']['id'])
    assert result is False

    bot.assets_client.patch_asset.side_effect = InvalidResponse()
    result = bot.patch_assets(lot, 'active', lot['data']['id'])
    assert result is False

    bot.assets_client.patch_asset.side_effect = Forbidden()
    result = bot.patch_assets(lot, 'active', lot['data']['id'])
    assert result is False

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Successfully patched asset 137738e77e1c4a968a2f1c4226639854 to active"
    assert log_strings[1] == "Failed to patch asset b30136280e2a4d8f8c3572d52b155741 to active (Not described error yet.)"
    assert log_strings[2] == "Failed to patch asset 137738e77e1c4a968a2f1c4226639854 to active (Not described error yet.)"
    assert log_strings[3] == "Failed to patch asset 137738e77e1c4a968a2f1c4226639854 to active (Not described error yet.)"


def test_patch_lot_success(bot, logger, mock_client):
    lot = {'data':
        {
            'status': 'waiting',
            'id': '35ad334815184a95a906e9b2020ae872',
            'assets': [
                "93312d374e4344b09f7c4876767b8f24",  # status: pending
            ]
        }
    }

    with open(ROOT + 'lots.json') as lots_data:
        lots = munchify(load(lots_data))

    lot_data = lots['35ad334815184a95a906e9b2020ae872']

    lot_data.data.status = 'active.pending'
    bot.assets_client.patch_lot.return_value = str(lot_data)
    result = bot.patch_lot(lot, 'active.pending')
    assert result is True

    lot_data.data.status = 'invalid'
    bot.assets_client.patch_lot.return_value = str(lot_data)
    result = bot.patch_lot(lot, 'invalid')
    assert result is True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Successfully patched lot 35ad334815184a95a906e9b2020ae872 to active.pending"
    assert log_strings[1] == "Successfully patched lot 35ad334815184a95a906e9b2020ae872 to invalid"


def test_patch_lot_failed(bot, logger, mock_client):
    lot = {'data':
        {
            'status': 'waiting',
            'id': '35ad334815184a95a906e9b2020ae872',
            'assets': [
                "93312d374e4344b09f7c4876767b8f24",  # status: pending
            ]
        }
    }

    bot.assets_client.patch_lot.side_effect = RequestFailed()
    result = bot.patch_lot(lot, 'active.pending')
    assert result is False

    bot.assets_client.patch_lot.side_effect = Forbidden()
    result = bot.patch_lot(lot, 'active.pending')
    assert result is False

    bot.assets_client.patch_lot.side_effect = InvalidResponse()
    result = bot.patch_lot(lot, 'active.pending')
    assert result is False

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Failed to patch lot 35ad334815184a95a906e9b2020ae872 to active.pending"
    assert log_strings[1] == "Failed to patch lot 35ad334815184a95a906e9b2020ae872 to active.pending"
    assert log_strings[2] == "Failed to patch lot 35ad334815184a95a906e9b2020ae872 to active.pending"


def test_process_lots_success(bot, logger, mocker):
    mock_database_view = mocker.patch.object(bot.db, 'view', autospec=True)
    with open(ROOT + 'lots_couchdb_view.json') as lots:
        mock_database_view.return_value = munchify(load(lots))['rows'][:3]
    lots = bot.get_lots(bot.bot_conf['LOTS_DB']['view'])
    mock_assets_available = mocker.patch.object(bot, 'check_lot_assets', autospec=True)
    mock_assets_available.side_effect = (value for value in (False, True, False))
    mock_make_patch = mocker.patch.object(bot, 'make_patch', autospec=True)

    bot.process_lots(lots)

    assert mock_assets_available.called is True
    assert mock_assets_available.call_count == 3
    assert mock_assets_available.call_args_list[0][0] == ({'data':
        {
            'status': 'dissolved',
            'id': 'fd122ba678174a19affcb3a0edc96e0e',
            'assets': ["a107b69548b54eb293ad82b36c6936a1"]
        }
                                                          },)
    assert mock_assets_available.call_args_list[1][0] == ({'data':
        {
            'status': 'dissolved',
            'id': 'c5cd5530af8547439d02dcde60750567',
            'assets': [
                "137738e77e1c4a968a2f1c4226639854",
                "d1d4c1985ec943d2b1a3db67cc577c62"
            ]
        }
                                                          },)
    assert mock_assets_available.call_args_list[2][0] == ({'data':
        {
            'status': 'waiting',
            'id': 'dd36079db10f4b77b8dd77ca64299e95',
            'assets': [
                "b30136280e2a4d8f8c3572d52b155741",
                "49faa46e7b344234aa3bbe75866b55a8",
                "d70982e5d32647d0847e9f2f9b26439c"
            ]
        }
                                                          },)

    assert mock_make_patch.called is True
    assert mock_make_patch.call_count == 3
    assert mock_make_patch.call_args_list[0][0] == ({'data':
        {
            'status': 'dissolved',
            'id': 'fd122ba678174a19affcb3a0edc96e0e',
            'assets': ["a107b69548b54eb293ad82b36c6936a1"]
        }
                                                    }, False)
    assert mock_make_patch.call_args_list[1][0] == ({'data':
        {
            'status': 'dissolved',
            'id': 'c5cd5530af8547439d02dcde60750567',
            'assets': [
                "137738e77e1c4a968a2f1c4226639854",
                "d1d4c1985ec943d2b1a3db67cc577c62"
            ]
        }
                                                    }, True)
    assert mock_make_patch.call_args_list[2][0] == ({'data':
        {
            'status': 'waiting',
            'id': 'dd36079db10f4b77b8dd77ca64299e95',
            'assets': [
                "b30136280e2a4d8f8c3572d52b155741",
                "49faa46e7b344234aa3bbe75866b55a8",
                "d70982e5d32647d0847e9f2f9b26439c"
            ]
        }
                                                    }, False)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Getting lots"
    assert log_strings[1] == "Got lots"
    assert log_strings[2] == "Processing lot fd122ba678174a19affcb3a0edc96e0e"
    assert log_strings[3] == "Processing lot c5cd5530af8547439d02dcde60750567"
    assert log_strings[4] == "Processing lot dd36079db10f4b77b8dd77ca64299e95"
    assert log_strings[5] == "Processed all lots"


def test_process_lots_failed(bot, logger, mocker):
    result = bot.process_lots(None)
    assert result is None

    lots = [{'data':
        {
            'status': 'dissolved',
            'id': 'fd122ba678174a19affcb3a0edc96e0e',
            'assets': ["a107b69548b54eb293ad82b36c6936a1"]
        }
    }]
    mock_assets_available = mocker.patch.object(bot, 'check_lot_assets', autospec=True)
    mock_assets_available.side_effect = RequestFailed()

    bot.process_lots(lots)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Could not get any lots"
    assert log_strings[1] == "Got lots"
    assert log_strings[2] == "Processing lot fd122ba678174a19affcb3a0edc96e0e"
    assert log_strings[3] == "Due to fail in getting assets, lot fd122ba678174a19affcb3a0edc96e0e is skipped"


def test_make_patch(bot, mocker):
    lot = {'data':
        {
            'status': 'waiting',
            'id': 'fd122ba678174a19affcb3a0edc96e0e',
            'assets': ["a107b69548b54eb293ad82b36c6936a1"]
        }
    }
    mock_patch_lot = mocker.patch.object(bot, 'patch_lot', autospec=True)
    mock_patch_assets = mocker.patch.object(bot, 'patch_assets', autospec=True)

    bot.make_patch(lot, True)

    assert mock_patch_lot.called is True
    assert mock_patch_lot.call_count == 1
    assert mock_patch_lot.call_args[0] == (
        {'data':
            {
                'status': 'waiting',
                'id': 'fd122ba678174a19affcb3a0edc96e0e',
                'assets': ['a107b69548b54eb293ad82b36c6936a1']
            }
        },
        'active.pending'
    )

    assert mock_patch_assets.called is True
    assert mock_patch_assets.call_count == 1
    assert mock_patch_assets.call_args[0] == (
        {'data':
            {
                'status': 'waiting',
                'id': 'fd122ba678174a19affcb3a0edc96e0e',
                'assets': ['a107b69548b54eb293ad82b36c6936a1']
            }
        },
        'active',
        'fd122ba678174a19affcb3a0edc96e0e'
    )

    bot.make_patch(lot, False)

    assert mock_patch_lot.call_count == 2
    assert mock_patch_lot.call_args[0] == (
        {'data':
            {
                'status': 'waiting',
                'id': 'fd122ba678174a19affcb3a0edc96e0e',
                'assets': ['a107b69548b54eb293ad82b36c6936a1']
            }
        },
        'invalid'
    )

    assert mock_patch_assets.call_count == 1

    lot['data']['status'] = 'dissolved'
    bot.make_patch(lot, False)

    assert mock_patch_lot.call_count == 2
    assert mock_patch_assets.call_count == 2
    assert mock_patch_assets.call_args[0] == (
        {'data':
            {
                'status': 'dissolved',
                'id': 'fd122ba678174a19affcb3a0edc96e0e',
                'assets': ['a107b69548b54eb293ad82b36c6936a1']
            }
        },
        'pending'
    )


@pytest.mark.skip(reason="infinite 'while' loop should be mocked properly")
def test_run(bot, logger, mocker, almost_always_true):
    mock_process_lots = mocker.patch.object(bot, 'process_lots', autospec=True)
    mock_sleep = mocker.patch.object(time, 'sleep', autospec=True)
    mock_true = mocker.patch('__builtin__.True', almost_always_true)
    bot.run()
