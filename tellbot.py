#!/usr/bin/env python3
# -*- coding: ascii -*-

import sys, os, re, time, operator, collections
import threading
import sqlite3

import basebot

REPLY_TIMEOUT = 3600

def seminormalize_nick(nick):
    return re.sub(r'\s+', '', nick)
def make_mention(nick):
    return '@' + re.sub(r'\s+', '', nick)

def format_list(l, fallback=None):
    l = tuple(l)
    if len(l) == 0:
        return fallback
    elif len(l) <= 2:
        return ' and '.join(l)
    else:
        return ', '.join(l[:-1]) + ', and ' + l[-1]

class OrderedSet:
    def __init__(self, base=(), key=lambda x: x):
        self.list = []
        self.set = set()
        self.key = key
        self.extend(base)

    def __bool__(self):
        return bool(self.list)
    def __nonzero__(self):
        return bool(self.list)

    def __len__(self):
        return len(self.list)

    def __contains__(self, item):
        return self.key(item) in self.set

    def __iter__(self):
        return iter(self.list)

    def copy(self):
        return self.__class__(self, key=self.key)

    def clear(self):
        self.list[:] = ()
        self.set.clear()

    def append(self, item):
        key = self.key(item)
        if key not in self.set:
            self.set.add(key)
            self.list.append(item)

    def extend(self, items):
        for item in items:
            self.append(item)

    def discard(self, item):
        key = self.key(item)
        if key in self.set:
            self.set.remove(key)
            self.list.remove(item)

    def discard_all(self, items):
        for item in items:
            self.discard(item)

class NotificationDistributor:
    def query_user(self, name): raise NotImplementedError
    def query_group(self, name): raise NotImplementedError
    def update_group(self, name, members): raise NotImplementedError
    def query_messages(self, user): raise NotImplementedError
    def pop_messages(self, user): raise NotImplementedError
    def add_message(self, user, message): raise NotImplementedError
    def query_delivery(self, msgid): raise NotImplementedError
    def add_delivery(self, msg, msgid, timestamp): raise NotImplementedError
    def gc(self): raise NotImplementedError

class NotificationDistributorMemory(NotificationDistributor):
    def __init__(self):
        self.groups = {}
        self.messages = {}
        self.deliveries = {}
        self.lock = threading.RLock()

    def query_user(self, name):
        return (basebot.normalize_nick(name), seminormalize_nick(name))

    def query_group(self, name):
        with self.lock:
            return self.groups.get(name, [])

    def update_group(self, name, members):
        with self.lock:
            self.groups[name] = members

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
            self.curs.execute('CREATE TABLE IF NOT EXISTS groups ('
                                  'groupname TEXT,'
                                  'member TEXT,'
                                  'name TEXT,'
                                  'PRIMARY KEY (groupname, member)'
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
        return (basebot.normalize_nick(name), seminormalize_nick(name))

    def query_group(self, name):
        with self.lock:
            self.curs.execute('SELECT member, name FROM groups '
                'WHERE groupname = ? ORDER BY _rowid_', (name,))
            return list(map(tuple, self.curs.fetchall()))

    def update_group(self, name, members):
        with self:
            self.curs.execute('DELETE FROM groups WHERE groupname = ?',
                              (name,))
            self.curs.executemany('INSERT INTO groups VALUES (?, ?, ?)',
                                  ((name, m, n) for m, n in members))

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
            self.curs.execute('SELECT _rowid_, * FROM messages '
                'WHERE delivered_to = ?', (msgid,))
            res = self.curs.fetchone()
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
        # Add a delivery notice.
        def handle_delivery(reply):
            m = seqs.pop(reply.id, None)
            if m: distr.add_delivery(m, reply.data.id, reply.data.time)

        distr, reply = self.manager.distributor, meta['reply']
        user = distr.query_user(msg['sender']['name'])[0]
        messages, now, seqs = distr.pop_messages(user), time.time(), {}

        # Deliver messages.
        for m in messages:
            seq = reply('[%s, %s ago] %s' % (make_mention(m['from']),
                basebot.format_delta(now - m['timestamp'], fractions=False),
                m['text']), handle_delivery)
            seqs[seq] = m

    def handle_command(self, cmdline, meta):
        # Common part of the argument parsers.
        def parse_userlist(base, groups, it, get_group=False):
            def abort():
                reply('Please specify a group first.')
                return Ellipsis, count
            count = 0
            for arg in it:
                if arg.startswith('@'): # Add user.
                    if get_group: return abort()
                    u = distr.query_user(arg[1:])
                    base.append(u)
                    groups[arg] = [u]
                    count += 1
                elif arg.startswith('*'): # Add group.
                    if get_group: return arg, count
                    g = distr.query_group(arg[1:])
                    base.extend(g)
                    groups[arg] = g
                    count += 1
                elif arg.startswith('+@'): # Add user (long form).
                    if get_group: return abort()
                    u = distr.query_user(arg[2:])
                    base.append(u)
                    groups[arg[1:]] = [u]
                    count += 1
                elif arg.startswith('+*'): # Add group (long form).
                    if get_group: return abort()
                    g = distr.query_group(arg[2:])
                    base.extend(g)
                    groups[arg[1:]] = g
                    count += 1
                elif arg.startswith('-@'): # Discard user.
                    if get_group: return abort()
                    base.discard(distr.query_user(arg[2:]))
                    count += 1
                elif arg.startswith('-*'): # Discard group.
                    if get_group: return abort()
                    base.discard_all(distr.query_group(arg[2:]))
                    count += 1
                elif arg.startswith('--'): # Option.
                    return arg, count
                elif arg.startswith('-'): # Avoid confusion with above.
                    reply('Single-letter options are not supported.')
                    return Ellipsis, count
                else: # Start of normal arguments.
                    return arg, count
            return None, count

        # Nickname formatting for output.
        def format_nick(item, ping):
            nnick = basebot.normalize_nick(item[1])
            print ((item, nnick, basebot.normalize_nick(sender),
                    basebot.normalize_nick(self.nickname)))
            if nnick == basebot.normalize_nick(sender):
                return 'yourself'
            elif nnick == basebot.normalize_nick(self.nickname):
                return 'myself'
            return (make_mention if ping else seminormalize_nick)(item[1])

        # A string representation of a list of users; arranged by group.
        def format_users(users, groups):
            if not users: return 'no-one'
            tr = lambda x: format_nick(x, True)
            users, seen, segments, add = users.copy(), set(), [], False
            for n, c in groups.items():
                nc = [i for i in c if i[0] not in seen]
                if n.startswith('@'):
                    if nc:
                        segments.extend(map(tr, nc))
                    else:
                        add = True
                else:
                    names = [tr(i) for i in nc]
                    if len(nc) == 0:
                        names.append('-already covered-')
                    elif len(nc) != len(c):
                        names.append('-already covered-')
                    segments.append('%s (%s)' % (n, format_list(names)))
                seen.update(i[0] for i in nc)
            if add: segments.append('-already covered-')
            return format_list(segments)

        # Reply with the users from a given list.
        def display_group(groupname, members, ping, comment):
            head = 'Members of *%s%s%s: ' % (groupname,
                ' ' if comment else '', comment)
            tr = lambda x: format_nick(x, ping)
            lst = format_list(map(tr, members), '-none-')
            reply(head + lst)

        basebot.Bot.handle_command(self, cmdline, meta)
        distr = self.manager.distributor
        sender, reply = meta['msg']['sender']['name'], meta['reply']

        # Send a message.
        if cmdline[0] == '!tell':
            # Parse arguments.
            recipients = OrderedSet(key=operator.itemgetter(0))
            groups, text = collections.OrderedDict(), None
            it = iter(cmdline[1:])
            while 1:
                arg, count = parse_userlist(recipients, groups, it)
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
            eff_recipients = tuple(basebot.normalize_nick(el[0])
                                   for el in recipients)

            # Abort if no text.
            if text is None:
                reply('Nothing will be delivered to %s.' %
                      format_users(recipients, groups))
                return

            # Schedule messages.
            base = {'text': text, 'from': sender, 'timestamp': time.time()}
            for user in eff_recipients:
                distr.add_message(user, dict(base, to=user))

            # Reply.
            reply('Message will be delivered to %s.' %
                  format_users(recipients, groups))

        # Reply to a freshly delivered message.
        elif cmdline[0] == '!reply':
            # Determine recipient.
            if meta['msg']['parent'] is None:
                reply('Nothing to reply to.')
                return
            cause = distr.query_delivery(meta['msg']['parent'])
            if cause is None:
                reply('Message not recognized.')
                return
            recipient = distr.query_user(cause['from'])

            # Abort if no text.
            if len(cmdline) == 1:
                reply('Nothing will be delivered.')
                return

            # Schedule message.
            text = meta['line'][cmdline[1].offset:]
            distr.add_message(cause['from'], {'text': text, 'from': sender,
                'timestamp': time.time(), 'to': recipient})

            # Inform user.
            reply('Message will be delivered.')

        # Update a group.
        elif cmdline[0] == '!tgroup':
            # Parse arguments.
            groupname, members, groups, ping = None, None, None, False
            it, count = iter(cmdline[1:]), 0
            while 1:
                arg, cnt = parse_userlist(members, groups, it,
                                          (groupname is None))
                count += cnt
                if arg is None:
                    break
                elif arg is Ellipsis:
                    return
                elif arg.startswith('*'):
                    groupname = arg[1:]
                    old_members = distr.query_group(groupname)
                    members = OrderedSet(old_members,
                                         key=operator.itemgetter(0))
                    groups = {}
                elif arg == '--ping':
                    ping = True
                elif arg.startswith('--') and arg != '--':
                    reply('Unknown option %s.' % arg)
                    return
                else:
                    reply('Please specify group changes only.')
                    return
            if groupname is None:
                reply('Please specify a group to show or change.')
                return

            # Display old membership.
            display_group(groupname, old_members, ping,
                          '' if count == 0 else 'before')
            if count == 0: return

            # Apply changes.
            distr.update_group(groupname, tuple(members))

            # Display new membership.
            display_group(groupname, members, ping, 'after')

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
