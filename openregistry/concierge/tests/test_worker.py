# -*- coding: utf-8 -*-
import os
import pytest

from copy import deepcopy
from json import load
from munch import munchify

from openprocurement_client.exceptions import (
    Forbidden,
    InvalidResponse,
    ResourceNotFound,
    RequestFailed
)

ROOT = os.path.dirname(__file__) + '/data/'


def test_get_lot(bot, logger, mocker):
    mock_continuous_changes_feed = mocker.patch('openregistry.concierge.worker.continuous_changes_feed', autospec=True)
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)
    mock_continuous_changes_feed.return_value = (lot['data'] for lot in lots)

    result = bot.get_lot()

    assert 'next' and '__iter__' in dir(result)  # assert generator object is returned

    assert result.next() == lots[0]['data']
    assert result.next() == lots[1]['data']

    with pytest.raises(StopIteration):
        result.next()

    mock_continuous_changes_feed.side_effect = Exception('Unexpected exception')
    assert bot.get_lot() is None

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Error while getting lots: Unexpected exception'


def test_run(bot, logger, mocker):
    mock_get_lot = mocker.patch.object(bot, 'get_lot', autospec=True)
    mock_process_lots = mocker.patch.object(bot, 'process_lots', autospec=True)
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)
    mock_get_lot.return_value = (lot['data'] for lot in lots)

    bot.run()

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Starting worker"

    assert mock_get_lot.called is True
    assert mock_process_lots.call_count == 2

    assert mock_process_lots.call_args_list[0][0][0] == lots[0]['data']
    assert mock_process_lots.call_args_list[1][0][0] == lots[1]['data']


def test_patch_lot(bot, logger, mocker):
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)
    mock_patch_resource_item = mocker.MagicMock()
    test_lot = deepcopy(lots[0])
    test_lot['data']['status'] = 'active.salable'
    mock_patch_resource_item.side_effect = [
        munchify(test_lot),
        Forbidden(response=munchify({"text": "Operation is forbidden."})),
        RequestFailed(response=munchify({"text": "Request failed."})),
        InvalidResponse(response=munchify({"text": "Invalid response."}))
    ]
    bot.lots_client.patch_resource_item = mock_patch_resource_item
    lot = lots[0]['data']
    status = 'active.salable'

    result = bot.patch_lot(lot=lot, status=status)
    assert result is True

    result = bot.patch_lot(lot=lot, status=status)
    assert result is False

    result = bot.patch_lot(lot=lot, status=status)
    assert result is False

    result = bot.patch_lot(lot=lot, status=status)
    assert result is False

    assert bot.lots_client.patch_resource_item.call_count == 4

    log_strings = logger.log_capture_string.getvalue().split('\n')

    assert log_strings[0] == 'Successfully patched lot 9ee8f769438e403ebfb17b2240aedcf1 to active.salable'
    assert log_strings[1] == 'Failed to patch lot 9ee8f769438e403ebfb17b2240aedcf1 to active.salable (Operation is forbidden.)'
    assert log_strings[2] == 'Failed to patch lot 9ee8f769438e403ebfb17b2240aedcf1 to active.salable (Request failed.)'
    assert log_strings[3] == 'Failed to patch lot 9ee8f769438e403ebfb17b2240aedcf1 to active.salable (Invalid response.)'


def test_patch_assets_pending_success(bot, logger, mocker):
    mock_patch_resource_item = mocker.MagicMock()
    bot.assets_client.patch_resource_item = mock_patch_resource_item

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    lot = lots[1]['data']
    status = 'pending'

    mock_patch_resource_item.side_effect = [
        munchify(assets[4]),
        munchify(assets[5]),
        munchify(assets[6]),
        munchify(assets[7])
    ]

    result = bot.patch_assets(lot=lot, status=status)
    assert result is True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Successfully patched asset 0a7eba27b22a454180d3a49b02a1842f to pending'
    assert log_strings[1] == 'Successfully patched asset 660cbb6e83c94c80baf47691732fd1b2 to pending'
    assert log_strings[2] == 'Successfully patched asset 8034c43e2d764006ad6e655e339e5fec to pending'
    assert log_strings[3] == 'Successfully patched asset 5545b519045a4637ab880f032960e034 to pending'

    assert bot.assets_client.patch_resource_item.call_count == 4


def test_patch_assets_pending_fail(bot, logger, mocker):
    mock_patch_resource_item = mocker.MagicMock()
    bot.assets_client.patch_resource_item = mock_patch_resource_item

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    lot = lots[1]['data']
    status = 'pending'

    mock_patch_resource_item.side_effect = [
        munchify(assets[4]),
        Forbidden(response=munchify({"text": "Operation is forbidden."})),
        munchify(assets[6]),
        munchify(assets[7])
    ]

    result = bot.patch_assets(lot=lot, status=status)
    assert result is False

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Successfully patched asset 0a7eba27b22a454180d3a49b02a1842f to pending'
    assert log_strings[1] == 'Failed to patch asset 660cbb6e83c94c80baf47691732fd1b2 to pending (Operation is forbidden.)'

    assert bot.assets_client.patch_resource_item.call_count == 2


def test_patch_assets_verification_success(bot, logger, mocker):
    mock_patch_resource_item = mocker.MagicMock()
    bot.assets_client.patch_resource_item = mock_patch_resource_item

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    lot = lots[0]['data']
    status = 'verification'

    mock_patch_resource_item.side_effect = [
        munchify(assets[0]),
        munchify(assets[1]),
        munchify(assets[2]),
        munchify(assets[3])
    ]

    result = bot.patch_assets(lot=lot, status=status, related_lot=lot['id'])
    assert result is True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Successfully patched asset e519404fd0b94305b3b19ec60add05e7 to verification'
    assert log_strings[1] == 'Successfully patched asset 64099f8259c64215b3bd290bc12ec73a to verification'
    assert log_strings[2] == 'Successfully patched asset f00d0ae5032f4927a4e0c046cafd3c62 to verification'
    assert log_strings[3] == 'Successfully patched asset c1c043ba1e3d457c8632c3b48c7279a4 to verification'

    assert bot.assets_client.patch_resource_item.call_count == 4


def test_patch_assets_active_fail(bot, logger, mocker):
    mock_patch_resource_item = mocker.MagicMock()
    bot.assets_client.patch_resource_item = mock_patch_resource_item

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    lot = lots[0]['data']
    status = 'verification'

    mock_patch_resource_item.side_effect = [
        munchify(assets[0]),
        munchify(assets[1]),
        RequestFailed(response=munchify({"text": "Request failed."})),
        munchify(assets[3])
    ]

    result = bot.patch_assets(lot=lot, status=status, related_lot=lot['id'])
    assert result is False

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Successfully patched asset e519404fd0b94305b3b19ec60add05e7 to verification'
    assert log_strings[1] == 'Successfully patched asset 64099f8259c64215b3bd290bc12ec73a to verification'
    assert log_strings[2] == 'Failed to patch asset f00d0ae5032f4927a4e0c046cafd3c62 to verification (Request failed.)'

    assert bot.assets_client.patch_resource_item.call_count == 3


def test_process_lots(bot, logger, mocker):
    mock_check_assets = mocker.patch.object(bot, 'check_assets', autospec=True)
    mock_check_assets.side_effect = [
        True, True,
        False,
        RequestFailed(response=munchify({"text": "Request failed."}))
    ]
    mock_patch_assets = mocker.patch.object(bot, 'patch_assets', autospec=True)
    mock_patch_assets.side_effect = [
        Exception('unexpected exception'),
        True, True, True, True
    ]
    mock_patch_lot = mocker.patch.object(bot, 'patch_lot', autospec=True)
    mock_patch_lot.return_value = True

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    verification_lot = lots[0]['data']
    dissolved_lot = lots[1]['data']

    # status == 'verification'
    bot.process_lots(verification_lot)  # assets_available: True; patch_assets: [raises exception, True]

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Processing lot 9ee8f769438e403ebfb17b2240aedcf1'
    assert log_strings[1] == 'Error while pathching assets: unexpected exception'

    assert mock_check_assets.call_count == 1
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_patch_assets.call_count == 2
    assert mock_patch_assets.call_args_list[0][0] == (verification_lot, 'verification', verification_lot['id'])
    assert mock_patch_assets.call_args_list[1][0] == (verification_lot, 'pending', verification_lot['id'])

    bot.process_lots(verification_lot)  # assets_available: True; patch_assets: [True, True]

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[2] == 'Processing lot 9ee8f769438e403ebfb17b2240aedcf1'

    assert mock_check_assets.call_count == 2
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_patch_assets.call_count == 4
    assert mock_patch_assets.call_args_list[2][0] == (verification_lot, 'verification', verification_lot['id'])
    assert mock_patch_assets.call_args_list[3][0] == (verification_lot, 'active', verification_lot['id'])

    assert mock_patch_lot.call_count == 1
    assert mock_patch_lot.call_args[0] == (verification_lot, 'active.salable')

    bot.process_lots(verification_lot)  # assets_available: False; patch_assets: None

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[3] == 'Processing lot 9ee8f769438e403ebfb17b2240aedcf1'

    assert mock_check_assets.call_count == 3
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_patch_lot.call_count == 2
    assert mock_patch_lot.call_args[0] == (verification_lot, 'pending')

    bot.process_lots(verification_lot)  # assets_available: raises exception; patch_assets: None

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[4] == 'Processing lot 9ee8f769438e403ebfb17b2240aedcf1'
    assert log_strings[5] == 'Due to fail in getting assets, lot 9ee8f769438e403ebfb17b2240aedcf1 is skipped'

    assert mock_check_assets.call_count == 4
    assert mock_check_assets.call_args[0] == (verification_lot,)

    # status == 'dissolved'
    bot.process_lots(dissolved_lot)  # assets_available: None; patch_assets: True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[6] == 'Processing lot b844573afaa24e4fb098f3027e605c87'

    assert mock_patch_assets.call_count == 5
    assert mock_patch_assets.call_args[0] == (dissolved_lot, 'pending')


def test_check_assets(bot, logger, mocker):
    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    verification_lot = deepcopy(lots[0]['data'])
    verification_lot['assets'] = ['e519404fd0b94305b3b19ec60add05e7']
    dissolved_lot = deepcopy(lots[1]['data'])
    dissolved_lot['assets'] = ["0a7eba27b22a454180d3a49b02a1842f"]

    mock_get_asset = mocker.MagicMock()
    mock_get_asset.side_effect = [
        RequestFailed(response=munchify({"text": "Request failed."})),
        ResourceNotFound(response=munchify({"text": "Asset could not be found."})),
        munchify(assets[0]),
        munchify(assets[7])
    ]

    bot.assets_client.get_asset = mock_get_asset

    with pytest.raises(RequestFailed):
        bot.check_assets(verification_lot)

    result = bot.check_assets(verification_lot)
    assert result is False

    result = bot.check_assets(verification_lot)
    assert result is True

    result = bot.check_assets(dissolved_lot)
    assert result is False

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Falied to get asset e519404fd0b94305b3b19ec60add05e7: Request failed."
    assert log_strings[1] == "Falied to get asset e519404fd0b94305b3b19ec60add05e7: Asset could not be found."
    assert log_strings[2] == "Successfully got asset e519404fd0b94305b3b19ec60add05e7"
    assert log_strings[3] == "Successfully got asset 0a7eba27b22a454180d3a49b02a1842f"
