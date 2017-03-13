#!/usr/bin/env python3
# -*- coding: ascii -*-

import sys, os, re, time
import threading
import sqlite3

import basebot

REPLY_TIMEOUT = 3600

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
    def query_delivery(self, msgid): raise NotImplementedError
    def add_delivery(self, msg, msgid, timestamp): raise NotImplementedError
    def gc(self): raise NotImplementedError

class NotificationDistributorMemory(NotificationDistributor):
    def __init__(self):
        self.messages = {}
        self.deliveries = {}
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
        message['id'] = id(message)
        with self.lock:
            self.messages.setdefault(user, []).append(message)

    def query_delivery(self, msgid):
        with self.lock:
            return self.deliveries[msgid]

    def add_delivery(self, msg, msgid, timestamp):
        with self.lock:
            for msg, msgid, timestamp in dels:
                msg['delivered_to'] = msgid
                msg['delivered'] = timestamp

    def gc(self):
        deadline = time.time() - REPLY_TIMEOUT
        with self.lock:
            for k, v in tuple(self.deliveries.items()):
                if v['delivered'] < deadline:
                    del self.deliveries[k]

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
                                  'timestamp REAL,'
                                  'delivered_to TEXT UNIQUE,'
                                  'delivered REAL'
                              ')')

    def _unwrap_message(self, item):
        return {'id': item[0], 'from': item[1], 'to': item[2],
                'text': item[3], 'timestamp': item[4],
                'delivered_to': item[5], 'delivered': item[6]}
    def _unwrap_messages(self, it):
        return list(map(self._unwrap_message, it))
    def _wrap_message(self, message):
        return (message.get('id'), message['from'], message['to'],
                message['text'], message['timestamp'],
                message.get('delivered_to'), message.get('delivered'))

    def query_user(self, name):
        return basebot.normalize_nick(name)

    def query_messages(self, user):
        user = basebot.normalize_nick(user)
        with self.lock:
            self.curs.execute('SELECT _rowid_, * FROM messages WHERE recipient = ? '
                'AND delivered_to IS NULL ORDER BY timestamp', (user,))
            return self._unwrap_messages(self.curs.fetchall())

    def pop_messages(self, user):
        user = basebot.normalize_nick(user)
        with self:
            self.curs.execute('SELECT _rowid_, sender, text, timestamp '
                'FROM messages WHERE recipient = ? AND delivered_to IS NULL '
                'ORDER BY timestamp', (user,))
            msgs = tuple(self.curs.fetchall())
            return self._unwrap_messages((i, s, user, c, t, None, None)
                                         for i, s, c, t in msgs)

    def add_message(self, user, message):
        user = basebot.normalize_nick(user)
        with self:
            self.curs.execute('INSERT INTO messages '
                'VALUES (?, ?, ?, ?, ?, ?)', self._wrap_message(message)[1:])

    def query_delivery(self, msgid):
        with self.lock:
            self.curs.execute('SELECT * FROM messages '
                'WHERE delivered_to = ?', (msgid,))
            res = self.curs.fetch()
            if res is None: return None
            return self._unwrap_message(res)

    def add_delivery(self, msg, msgid, timestamp):
        with self:
            self.curs.execute('UPDATE messages SET delivered_to = ?, '
                'delivered = ? WHERE _rowid_ = ?', (msgid, timestamp,
                                                    msg['id']))

    def gc(self):
        deadline = time.time() - REPLY_TIMEOUT
        with self:
            self.curs.execute('DELETE FROM messages WHERE delivered < ?',
                              (deadline,))

class TellBot(basebot.Bot):
    BOTNAME = 'TellBot'
    NICKNAME = 'TellBot'

    def handle_chat(self, msg, meta):
        def handle_delivery(reply):
            m = seqs.pop(reply.id, None)
            if m: distr.add_delivery(m, reply.data.id, reply.data.time)
        distr, reply = self.manager.distributor, meta['reply']
        user = distr.query_user(msg['sender']['name'])
        messages = distr.pop_messages(user)
        now = time.time()
        seqs = {}
        for m in messages:
            seq = reply('[%s, %s ago] %s' % (make_mention(m['from']),
                basebot.format_delta(now - m['timestamp'], fractions=False),
                m['text']), handle_delivery)
            seqs[seq] = m

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

class GCThread(threading.Thread):
    def __init__(self, distr):
        threading.Thread.__init__(self)
        self.distr = distr
        self.exiting = False
        self.cond = threading.Condition()

    def shutdown(self):
        with self.cond:
            self.exiting = True
            self.cond.notifyAll()

    def run(self):
        cont = True
        while cont:
            self.distr.gc()
            wakeup = time.time() + REPLY_TIMEOUT
            with self.cond:
                while not self.exiting:
                    now = time.time()
                    if now >= wakeup: break
                    self.cond.wait(wakeup - now)
                else:
                    break

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
        self.children.append(GCThread(self.distributor))

if __name__ == '__main__': basebot.run_main(TellBot, mgrcls=TellBotManager)
