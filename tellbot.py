#!/usr/bin/env python3
# -*- coding: ascii -*-

import sys, os, re, time
import operator, collections
import fnmatch
import threading
import sqlite3

import basebot

REPLY_TIMEOUT = 3600

HELP_TEXT = '''
To add a message to other users' mailbox, use
    !tell @user1 [@user2 ...] [*group1 ...] message
    !tnotify @user1 [@user2 ...] [*group1 ...] message
To create or grow, or shrink a group of users, use
    !tgroup *group @user1 [@user2 ...] [*group1 ...]
    !tgroup *group -@user1 [-@user2 ...] [-*group1 ...]
To list available groups, use
    !tgrouplist
To check when a user was last online, use
    !seen @user
For a thorough manual, see https://github.com/CylonicRaider/tellbot.
'''[1:-1]

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
    def query_seen(self, user): raise NotImplementedError
    def update_seen(self, user, name, time): raise NotImplementedError
    def list_groups(self): raise NotImplementedError
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
        self.last_seen = {}
        self.messages = {}
        self.deliveries = {}
        self.groups = {}
        self.lock = threading.RLock()

    def query_user(self, name):
        return (basebot.normalize_nick(name), seminormalize_nick(name))

    def query_seen(self, user):
        with self.lock:
            return self.last_seen.get(user)

    def update_seen(self, user, name, time):
        with self.lock:
            self.last_seen[user] = (name, time)

    def list_groups(self):
        with self.lock:
            return list(self.groups)

    def query_group(self, name):
        with self.lock:
            return self.groups.get(name, [])

    def update_group(self, name, members):
        with self.lock:
            self.groups[name] = members

    def query_messages(self, user):
        with self.lock:
            return self.messages.get(user, [])

    def pop_messages(self, user):
        with self.lock:
            return self.messages.pop(user, [])

    def add_message(self, user, message):
        message['id'] = id(message)
        message['to'] = user
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
                                  'reason TEXT,'
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
            self.curs.execute('CREATE TABLE IF NOT EXISTS seen ('
                                  'user TEXT PRIMARY KEY,'
                                  'name TEXT,'
                                  'timestamp REAL'
                              ')')

    def _unwrap_message(self, item):
        return {'id': item[0], 'from': item[1], 'to': item[2],
                'reason': item[3], 'text': item[4], 'timestamp': item[5],
                'delivered_to': item[6], 'delivered': item[7]}
    def _unwrap_messages(self, it):
        return list(map(self._unwrap_message, it))
    def _wrap_message(self, message):
        return (message.get('id'), message['from'], message['to'],
                message['reason'], message['text'], message['timestamp'],
                message.get('delivered_to'), message.get('delivered'))

    def query_user(self, name):
        return (basebot.normalize_nick(name), seminormalize_nick(name))

    def query_seen(self, user):
        with self.lock:
            self.curs.execute('SELECT name, timestamp FROM seen '
                'WHERE user = ?', (user,))
            return self.curs.fetchone()

    def update_seen(self, user, name, timestamp):
        with self:
            self.curs.execute('INSERT OR REPLACE INTO seen VALUES (?, ?, ?)',
                (user, name, timestamp))

    def list_groups(self):
        with self.lock:
            self.curs.execute('SELECT DISTINCT groupname FROM groups')
            return list(i[0] for i in self.curs.fetchall())

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
        with self.lock:
            self.curs.execute('SELECT _rowid_, * FROM messages '
                'WHERE recipient = ? AND delivered_to IS NULL '
                'ORDER BY timestamp', (user,))
            return self._unwrap_messages(self.curs.fetchall())

    def pop_messages(self, user):
        with self:
            self.curs.execute('SELECT _rowid_, sender, reason, text, '
                'timestamp FROM messages WHERE recipient = ? '
                'AND delivered_to IS NULL ORDER BY timestamp', (user,))
            msgs = tuple(self.curs.fetchall())
            return self._unwrap_messages((i, s, user, w, c, t, None, None)
                                         for i, s, w, c, t in msgs)

    def add_message(self, user, message):
        message['to'] = user
        with self:
            self.curs.execute('INSERT INTO messages '
                'VALUES (?, ?, ?, ?, ?, ?, ?)',
                self._wrap_message(message)[1:])

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
    SHORT_HELP = 'I can schedule messages to be delivered to other users.'
    LONG_HELP = HELP_TEXT

    def _format_nick(self, nick, ping=True, subject=None):
        nnick = basebot.normalize_nick(nick)
        if subject and nnick == basebot.normalize_nick(subject):
            return 'yourself'
        elif nnick == basebot.normalize_nick(self.nickname):
            return 'myself'
        return (make_mention if ping else seminormalize_nick)(nick)

    def _format_users(self, users, groups, subject, prevent_self=False):
        if not users: return ('no-one', {})
        tr = lambda x: self._format_nick(x[1], True, subject[1])
        seen, segnames, segments, reasons = set(), [], {}, {}
        for n, c in groups.items():
            if n.startswith('@'):
                el = c[0]
                if reasons.get(el[0], '').startswith('*'):
                    del segments[reasons[el[0]]][el[0]]
                reasons[el[0]] = n
                segnames.append(tr(el))
                seen.add(n)
            else:
                nc = [i for i in c if i[0] not in seen]
                if subject in nc and prevent_self:
                    nc.remove(subject)
                    reasons.pop(subject[0], None)
                    users.discard(subject)
                for normnick, nick in nc:
                    reasons[normnick] = n
                segnames.append(n)
                segments[n] = collections.OrderedDict(
                    (i[0], tr(i)) for i in nc)
                seen.update(i[0] for i in nc)
        parts = []
        for n in segnames:
            if n not in segments:
                parts.append(n)
                continue
            names = list(segments[n].values())
            if not groups[n] or len(names) != len(groups[n]):
                names.append('...')
            parts.append('%s (%s)' % (n, format_list(names)))
        return (format_list(parts, 'no-one'), reasons)

    def handle_chat_ex(self, msg, meta):
        # Format a nickname.
        def format_nick(name):
            return self._format_nick(name, True, user[1])

        # Format a delivery reason.
        def format_reason(src):
            if src.startswith('<re> '):
                res = format_reason(src[5:])
                return ' replying' + res
            elif src.startswith('@'):
                return ' to ' + format_nick(src[1:])
            else:
                return ' to ' + src

        # Add a delivery notice.
        def handle_delivery(reply):
            m = seqs.pop(reply.id, None)
            if m: distr.add_delivery(m, reply.data.id, reply.data.time)

        basebot.Bot.handle_chat_ex(self, msg, meta)
        distr, reply = self.manager.distributor, meta['reply']
        user, now = distr.query_user(msg['sender']['name']), time.time()

        # Update online time database.
        if not meta['edit'] and not meta['long']:
            distr.update_seen(user[0], user[1], now)

        # Deliver messages.
        if not meta['live']: return
        messages, seqs = distr.pop_messages(user[0]), {}
        for m in messages:
            seq = reply('[From %s%s, %s ago] %s' % (format_nick(m['from']),
                format_reason(m['reason']), basebot.format_delta(now -
                m['timestamp'], fractions=False), m['text']),
                handle_delivery)
            seqs[seq] = m

    def send_notify(self, distr, sender, recipients, groups, text, reply,
                    reason=None):
        # Prevent messages to oneself unless explicit.
        reclist, reasons = self._format_users(recipients, groups, sender,
                                              True)

        # Format fancy recipient list.
        text = (text or '').strip()
        if not text:
            reply('Will not tell %s.' % reclist)
            return

        # Schedule messages.
        base = {'text': text, 'from': sender[1], 'timestamp': time.time()}
        for user, nick in recipients:
            cur_reason = reason or reasons[user]
            distr.add_message(user, dict(base, reason=cur_reason))

        # Reply.
        reply('Will tell %s.' % reclist)

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
            return self._format_nick(item[1], ping, sender[1])

        # Reply with the users from a given list.
        def display_group(groupname, members, ping, comment):
            head = 'Members of *%s%s%s%s: ' % (groupname,
                ' ' if comment else '', comment,
                ' (%s)' % len(members) if members else '')
            tr = lambda x: format_nick(x, ping)
            lst = format_list(map(tr, members), '-none-')
            reply(head + lst)

        basebot.Bot.handle_command(self, cmdline, meta)
        distr = self.manager.distributor
        sender = distr.query_user(meta['msg']['sender']['name'])
        reply = meta['reply']

        # Send a message.
        if cmdline[0] in ('!tell', '!tnotify'):
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

            # Actual hauling outlined into own function.
            self.send_notify(distr, sender, recipients, groups, text, reply)

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

            # Send message.
            self.send_notify(
                distr,
                sender,
                OrderedSet((recipient,), key=operator.itemgetter(0)),
                {'@' + recipient[0]: [recipient]},
                meta['line'][cmdline[1].offset:],
                reply,
                '<re> ' + format_nick(recipient, True))

        # Reply to a group.
        elif cmdline[0] == '!reply-all':
            # Determine recipient.
            if meta['msg']['parent'] is None:
                reply('Nothing to reply to.')
                return
            cause = distr.query_delivery(meta['msg']['parent'])
            if cause is None:
                reply('Message not recognized.')
                return
            reason = cause['reason']
            if reason.startswith('<re> '): reason = reason[5:]

            # Determine group members.
            if reason.startswith('@'):
                groups = {reason: [distr.query_user(reason[1:])]}
            else:
                groups = {reason: distr.query_group(reason[1:])}
            recipients = OrderedSet(groups[reason],
                                    key=operator.itemgetter(0))

            # Send message.
            self.send_notify(distr, sender, recipients, groups,
                meta['line'][cmdline[1].offset:], reply, '<re> ' + reason)

        # Enumerate available groups.
        elif cmdline[0] == '!tgrouplist':
            # Parse arguments.
            if len(cmdline) == 1:
                filt = lambda x: True
                filt_all = True
            elif len(cmdline) == 2:
                filt = re.compile(fnmatch.translate(cmdline[1]), re.I).match
                filt_all = False
            else:
                reply('Please specify a matching pattern or nothing.')
                return

            # Obtain list.
            names = ['*' + i for i in distr.list_groups() if filt(i)]
            names.sort()

            if not names:
                reply('No groups.' if filt_all else
                      'No groups mathing pattern.')
                return

            # Group by first character.
            groups = []
            for n in names:
                if not groups or n[:2] != groups[-1][-1][:2]:
                    groups.append([n])
                else:
                    groups[-1].append(n)

            # Output.
            reply('\n'.join(map(', '.join, groups)))

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

        # When was a user last active?
        elif cmdline[0] == '!seen':
            # Parse arguments.
            users, groups = OrderedSet(key=operator.itemgetter(0)), {}
            it = iter(cmdline[1:])
            while 1:
                arg, cnt = parse_userlist(users, groups, it)
                if arg is None:
                    break
                elif arg is Ellipsis:
                    return
                elif arg.startswith('--'):
                    reply('Please specify users or groups only.')
                    return

            # Handle empty list.
            if not users:
                reply('No-one to check for.')
                return

            # Output information.
            now, bnn = time.time(), basebot.normalize_nick
            for user, nick in users:
                seen = distr.query_seen(user)
                fnick = format_nick((user, nick), True)
                if fnick[:1].islower(): fnick = fnick[0].upper() + fnick[1:]
                if seen is None:
                    reply('%s not seen.' % fnick)
                    continue
                if bnn(nick) != bnn(seen[0]):
                    comment = ' (as %s)' % format_nick(seen[0], True)
                else:
                    comment = ''
                reply('%s%s last seen %s ago.' % (fnick, comment,
                    basebot.format_delta(now - seen[1])))

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
                              'persistence (default in-memory)')

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
