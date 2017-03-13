#!/usr/bin/env python3
# -*- coding: ascii -*-

import sys, os, time
import threading

import basebot

class OrderedSet:
    def __init__(self, base=()):
        self.list = []
        self.set = set()
        self.add(*base)

    def __iter__(self):
        return iter(self.list)

    def clear(self):
        self.list[:] = ()
        self.set.clear()

    def append(self, item):
        if item not in self.set:
            self.set.add(item)
            self.list.append(item)

    def extend(self, items):
        for item in items:
            self.append(item)

    def discard(self, item):
        if item in self.set:
            self.set.remove(item)
            self.list.remove(item)

    def discard_all(self, items):
        for item in items:
            self.discard(item)

class NotificationDistributor:
    def __init__(self):
        self.messages = {}
        self.lock = threading.RLock()

    def query_users(self, name):
        return ()

    def query_messages(self, user):
        pass

    def pop_messages(self, user):
        pass

    def add_message(self, user, message):
        pass

class TellBot(basebot.Bot):
    def handle_command(self, cmdline, meta):
        def parse_userlist(base, it):
            for arg in it:
                if arg.startswith('@'): # Add user.
                    base.extend(distr.query_users(arg[1:]))
                elif arg.startswith('*'): # Add group.
                    reply('Groups are NYI.')
                    return Ellipsis
                elif arg.startswith('+@'): # Add user (long form).
                    base.extend(distr.query_users(arg[2:]))
                elif arg.starstwith('+*'): # Add group (long form).
                    reply('Groups are NYI.')
                    return Ellipsis
                elif arg.startswith('-@'): # Discard user.
                    base.discard_all(distr.query_users(arg[2:]))
                elif arg.startswith('-*'): # Discard group.
                    reply('Groups are NYI.')
                    return Ellipsis
                elif arg.startswith('--'): # Option.
                    return arg
                elif arg.startswith('-'): # Avoid confusion with above.
                    reply('Single-letter options are not supported.')
                    return Ellipsis
                else: # Start of normal arguments.
                    return arg

        basebot.Bot.handle_command(self, cmdline, meta)
        distr, reply = self.manager.distributor, meta['reply']

        if cmdline[0] == '!tell':
            # Parse arguments.
            recipients, text, it = OrderedSet(), None, iter(cmdline[1:])
            while 1:
                arg = parse_userlist(it)
                if arg is None:
                    break
                elif arg is Ellipsis:
                    return
                elif arg == '--':
                    try:
                        text = meta['line'][next(it).offset:]
                    except StopIteration:
                        pass
                elif arg.startswith('--'):
                    reply('Unknown option %s.' % arg)
                    return
                else:
                    text = meta['line'][arg.offset:]
            recipients = tuple(recipients)

            # Abort if no text.
            if text is None:
                reply('Nothing will be delivered.')
                return

            # Collect metadata.
            message = {'text': text, 'timestamp': time.time()}

            # Schedule message.
            for user in recipients:
                distr.add_message(user, message)

            # Reply.
            if recipients:
                reply('Message will be delivered.')
            else:
                reply('Message will be delivered to no-one.')


class TellBotManager(basebot.BotManager):
    def __init__(self, **config):
        basebot.BotManager.__init__(self, **config)
        self.distributor = NotificationDistributor()

if __name__ == '__main__': basebot.run_main(TellBot, mgrcls=TellBotManager)
