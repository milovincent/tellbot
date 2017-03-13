#!/usr/bin/env python3
# -*- coding: ascii -*-

import sys, os

import basebot

class TellBot(basebot.Bot):
    pass

class TellBotManager(basebot.BotManager):
    pass

if __name__ == '__main__': basebot.run_main(TellBot, mgrcls=TellBotManager)
