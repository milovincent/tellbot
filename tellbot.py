#!/usr/bin/env python3
# -*- coding: ascii -*-

import sys, os, re, time
import threading
import sqlite3

import basebot

def make_mention(nick):
    return '@' + re.sub(r'\s+', '', nick)

class OrderedSet:
    def __init__(self, base=()):
        self.list = []
        self.set = set()
        self.extend(base)

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
    def query_user(self, name): raise NotImplementedError
    def query_messages(self, user): raise NotImplementedError
    def pop_messages(self, user): raise NotImplementedError
    def add_message(self, user, message): raise NotImplementedError

class NotificationDistributorMemory(NotificationDistributor):
    def __init__(self):
        self.messages = {}
        self.lock = threading.RLock()

    def query_user(self, name):
        return basebot.normalize_nick(name)

    def query_messages(self, user):
        user = basebot.normalize_nick(user)
        with self.lock:
            return self.messages.get(user, [])

    def pop_messages(self, user):
        user = basebot.normalize_nick(user)
        with self.lock:
            return self.messages.pop(user, [])

    def add_message(self, user, message):
        user = basebot.normalize_nick(user)
        with self.lock:
            self.messages.setdefault(user, []).append(message)

class NotificationDistributorSQLite(NotificationDistributor):
    def __init__(self, filename):
        self.filename = filename
        self.lock = threading.RLock()
        self.conn = None
        self.curs = None
        self.init()

    def __enter__(self):
        return self.lock.__enter__()

    def __exit__(self, *args):
        try:
            self.conn.commit()
        finally:
            return self.lock.__exit__()

    def init(self):
        with self.lock:
            self.conn = sqlite3.connect(self.filename, isolation_level='',
                                        check_same_thread=False)
            self.curs = self.conn.cursor()
            self.curs.execute('CREATE TABLE IF NOT EXISTS messages ('
                                  'sender TEXT,'
                                  'recipient TEXT,'
                                  'text TEXT,'
                                  'timestamp REAL'
                              ')')

    def _unwrap_messages(self, it):
        ret = []
        for item in it:
            ret.append({'from': item[0], 'to': item[1],
                        'text': item[2], 'timestamp': item[3]})
        return ret
    def _wrap_message(self, message):
        return (message['from'], message['to'], message['text'],
                message['timestamp'])

    def query_user(self, name):
        return basebot.normalize_nick(name)

    def query_messages(self, user):
        user = basebot.normalize_nick(user)
        with self.lock:
            self.curs.execute('SELECT * FROM message WHERE recipient = ?',
                              (user,))
            return self._unwrap_messages(self.curs.fetchall())

    def pop_messages(self, user):
        user = basebot.normalize_nick(user)
        with self:
            self.curs.execute('SELECT _rowid_, sender, text, '
                'timestamp FROM messages WHERE recipient = ?', (user,))
            msgs = tuple(self.curs.fetchall())
            self.curs.executemany('DELETE FROM messages WHERE _rowid_ = ?',
                                  (str(el[0]) for el in msgs))
            return self._unwrap_messages(
                (s, user, c, t) for r, s, c, t in msgs)

    def add_message(self, user, message):
        user = basebot.normalize_nick(user)
        with self:
            self.curs.execute('INSERT INTO messages (sender, recipient, '
                'text, timestamp) VALUES (?, ?, ?, ?)',
                self._wrap_message(message))

class TellBot(basebot.Bot):
    BOTNAME = 'TellBot'
    NICKNAME = 'TellBot'

    def handle_chat(self, msg, meta):
        distr, reply = self.manager.distributor, meta['reply']
        user = distr.query_user(msg['sender']['name'])
        messages = distr.pop_messages(user)
        now = time.time()
        for m in messages:
            reply('[%s, %s ago] %s' % (make_mention(m['from']),
                basebot.format_delta(now - m['timestamp'], fractions=False),
                m['text']))

    def handle_command(self, cmdline, meta):
        def parse_userlist(base, it):
            for arg in it:
                if arg.startswith('@'): # Add user.
                    base.append(distr.query_user(arg[1:]))
                elif arg.startswith('*'): # Add group.
                    reply('Groups are NYI.')
                    return Ellipsis
                elif arg.startswith('+@'): # Add user (long form).
                    base.append(distr.query_user(arg[2:]))
                elif arg.startswith('+*'): # Add group (long form).
                    reply('Groups are NYI.')
                    return Ellipsis
                elif arg.startswith('-@'): # Discard user.
                    base.discard(distr.query_user(arg[2:]))
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
                arg = parse_userlist(recipients, it)
                if arg is None:
                    break
                elif arg is Ellipsis:
                    return
                elif arg == '--':
                    try:
                        text = meta['line'][next(it).offset:]
                    except StopIteration:
                        pass
                    break
                elif arg.startswith('--'):
                    reply('Unknown option %s.' % arg)
                    return
                else:
                    text = meta['line'][arg.offset:]
                    break
            recipients = tuple(recipients)

            # Abort if no text.
            if text is None:
                reply('Nothing will be delivered.')
                return

            # Collect metadata.
            message = {'text': text, 'from': meta['msg']['sender']['name'],
                       'timestamp': time.time()}

            # Schedule message.
            for user in recipients:
                distr.add_message(user, dict(message, to=user))

            # Reply.
            if recipients:
                reply('Message will be delivered.')
            else:
                reply('Message will be delivered to no-one.')

class TellBotManager(basebot.BotManager):
    @classmethod
    def prepare_parser(cls, parser, config):
        basebot.BotManager.prepare_parser(parser, config)
        parser.add_option('--db', dest='db', metavar='<path>',
                          help='SQLite database file for message '
                              'persistence (deault in-memory)')

    @classmethod
    def interpret_args(cls, options, arguments, config):
        bots, config = basebot.BotManager.interpret_args(options,
            arguments, config)
        for name in ('db',):
            value = getattr(options, name)
            if value is not None:
                config[name] = value
        return (bots, config)

    def __init__(self, **config):
        basebot.BotManager.__init__(self, **config)
        self.db = config.get('db', None)
        if self.db:
            self.distributor = NotificationDistributorSQLite(self.db)
        else:
            self.distributor = NotificationDistributorMemory()

if __name__ == '__main__': basebot.run_main(TellBot, mgrcls=TellBotManager)
