# Registry for channels and their source parsers
CHANNEL_REGISTRY = {
    "ATM": {
        "sources": {
            "SWITCH": "app.channels.atm.parser_switch.SwitchParser",
            "CBS": "app.channels.atm.parser_cbs.CBSParser",
            "EJ": "app.channels.atm.parser_ej.EJParser",
            "NETWORK": "app.channels.atm.parser_network.NetworkParser",
            "SETTLEMENT": "app.channels.atm.parser_settlement.SettlementParser"
        },
        "normalizer": "app.channels.atm.normalizer.ATMNormalizer",
        "matcher": "app.channels.atm.matcher.ATMMatcher"
    },
    "POS": {
        "sources": {
            "SWITCH": "app.channels.pos.parser_switch.PosSwitchParser",
            "CBS": "app.channels.pos.parser_cbs.PosCBSParser",
            "SETTLEMENT": "app.channels.pos.parser_settlement.PosSettlementParser"
        },
        "normalizer": "app.channels.pos.normalizer.POSNormalizer",
        "matcher": "app.channels.pos.matcher.POSMatcher"
    },
    "CARDS": {
        "sources": {
            "NETWORK": "app.channels.cards.parser_network.CardNetworkParser",
            "CBS": "app.channels.cards.parser_cbs.CardCBSParser",
            "SETTLEMENT": "app.channels.cards.parser_settlement.CardSettlementParser"
        },
        "normalizer": "app.channels.cards.normalizer.CardsNormalizer",
        "matcher": "app.channels.cards.matcher.CardsMatcher"
    },
    "WALLET": {
        "sources": {
            "PLATFORM": "app.channels.wallet.parser_platform.WalletPlatformParser",
            "CBS": "app.channels.wallet.parser_cbs.WalletCBSParser"
        },
        "normalizer": "app.channels.wallet.normalizer.WalletNormalizer",
        "matcher": "app.channels.wallet.matcher.WalletMatcher"
    },
    "MOBILE_MONEY": {
        "sources": {
            "PLATFORM": "app.channels.mobile_money.parser_platform.MobileMoneyParser",
            "CBS": "app.channels.mobile_money.parser_cbs.MobileMoneyCBSParser"
        },
        "normalizer": "app.channels.mobile_money.normalizer.MobileMoneyNormalizer",
        "matcher": "app.channels.mobile_money.matcher.MobileMoneyMatcher"
    }
}
