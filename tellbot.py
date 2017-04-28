#!/usr/bin/env python3
# -*- coding: ascii -*-

import sys, os, re, time
import operator, collections
import fnmatch
import threading
import sqlite3

import basebot

REPLY_TIMEOUT = 3600
GC_INTERVAL = 3600

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

def titlefirst(s):
    if not s: return ''
    return s[0].upper() + s[1:]

def format_list(l, fallback=None):
    l = tuple(l)
    if len(l) == 0:
        return fallback
    elif len(l) <= 2:
        return ' and '.join(l)
    else:
        return ', '.join(l[:-1]) + ', and ' + l[-1]

class OrderedSet:
    @classmethod
    def firstel(cls, base=()):
        return cls(base, operator.itemgetter(0))

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

    def sort(self, key=None, reverse=False):
        if key is None: key = self.key
        self.list.sort(key=key, reverse=reverse)

class DBLock:
    class Committer:
        def __init__(self, parent):
            self.parent = parent

        def __enter__(self):
            self.parent.acquire(commit=True)

        def __exit__(self, t, v, tb):
            self.parent.release()

    def __init__(self, conn=None):
        self.lock = threading.RLock()
        self.conn = conn
        self.commit = False
        self.counter = 0
        self.committing = self.Committer(self)

    def __enter__(self):
        self.acquire()

    def __exit__(self, t, v, tb):
        self.release()

    def acquire(self, blocking=True, commit=False):
        ret = self.lock.acquire(blocking)
        if commit: self.commit = True
        print ('[acquire] %r %r' % (self.counter, self.commit))
        self.counter += 1
        return ret

    def release(self):
        if not self.lock._is_owned():
            raise RuntimeError('Trying to release foreign lock!')
        self.counter -= 1
        print ('[released] %r %r' % (self.counter, self.commit))
        if self.counter == 0 and self.commit:
            if self.conn:
                print ('[committing]')
                self.conn.commit()
            self.commit = False
        return self.lock.release()

class NotificationDistributor:
    def normalize_user(self, name):
        return (basebot.normalize_nick(name), seminormalize_nick(name))
    def query_user(self, name):
        raise NotImplementedError
    def query_seen(self, user):
        raise NotImplementedError
    def update_seen(self, user, name, time, unread):
        raise NotImplementedError
    def query_aliases(self, base):
        raise NotImplementedError
    def add_aliases(self, base, names):
        raise NotImplementedError
    def remove_aliases(self, base, names):
        raise NotImplementedError
    def list_groups(self):
        raise NotImplementedError
    def query_group(self, name):
        raise NotImplementedError
    def update_group(self, name, members):
        raise NotImplementedError
    def message_bounds(self, user):
        raise NotImplementedError
    def query_messages(self, user, stale=False):
        raise NotImplementedError
    def pop_messages(self, user, stale=False):
        raise NotImplementedError
    def add_message(self, user, message):
        raise NotImplementedError
    def query_delivery(self, msgid):
        raise NotImplementedError
    def add_delivery(self, msg, msgid, timestamp):
        raise NotImplementedError
    def gc(self):
        raise NotImplementedError

class NotificationDistributorMemory(NotificationDistributor):
    def __init__(self):
        self.aliases = {}
        self.revaliases = {}
        self.seen = {}
        self.messages = {}
        self.deliveries = {}
        self.groups = {}
        self.revgroups = {}
        self.lock = threading.RLock()

    def query_user(self, name):
        ret = self.normalize_user(name)
        try:
            return (self.revaliases[ret[0]], ret[1])
        except KeyError:
            return ret

    def query_seen(self, user):
        with self.lock:
            return self.seen.get(user)

    def update_seen(self, user, name, time, unread, room):
        with self.lock:
            oldent = self.seen.get(user, (None, None, 0, None))
            self.seen[user] = [name, time,
                oldent[2] if unread is None else unread, room]
            return (unread != oldent[2])

    def query_aliases(self, base):
        with self.lock:
            return self.aliases.get(base, [])

    def add_aliases(self, base, names):
        with self.lock:
            # Collect effective new names.
            effnames, bases = OrderedSet.firstel(), set()
            for n in names:
                effnames.append(n)
                bn = self.revaliases.get(n[0])
                if not bn: continue
                effnames.extend(self.aliases[bn])
                bases.add(bn)
            # Remove old alias tables.
            for b in bases: self.aliases.pop(b, None)
            # Create new alias table.
            self.aliases[base] = list(effnames)
            # Repoint individual entries.
            for e in effnames: self.revaliases[e[0]] = base
            # Rewrite groups.
            groups = set().union(*(self.revgroups.get(i, ()) for i in bases))
            for n in groups:
                ng = OrderedSet(self.groups[n],
                    key=lambda x: self.revaliases.get(x[0], x[0]))
                self.groups[n] = [(base, el[1]) if el[0] in bases else el
                                  for el in ng]
            for b in bases:
                self.revgroups.pop(b, None)
            self.revgroups[base] = groups
            # Update seen.
            entry, unread = None, 0
            for b in bases:
                s = self.seen.pop(b, None)
                if not s: continue
                unread += s[2]
                if entry is None or s[1] > entry[1]:
                    entry = s
            if entry:
                self.seen[base] = [entry[0], entry[1], unread, entry[3]]

    def remove_aliases(self, base, names):
        with self.lock:
            if base not in self.aliases: return
            rms = set(n[0] for n in names)
            newnames, removed = [], []
            for el in self.aliases[base]:
                (removed if el[0] in rms else newnames).append(el)
            self.aliases[base] = newnames
            for el in removed:
                self.revaliases.pop(el[0], None)

    def list_groups(self):
        with self.lock:
            return list(self.groups)

    def query_group(self, name):
        with self.lock:
            return self.groups.get(name, [])

    def update_group(self, name, members):
        with self.lock:
            for e in self.groups.get(name, ()):
                g = self.revgroups[e[0]]
                g.discard(name)
            self.groups[name] = members
            for e in members:
                try:
                    g = self.revgroups[e[0]]
                except KeyError:
                    g = set()
                    self.revgroups[e[0]] = g
                g.add(name)

    def message_bounds(self, user):
        with self.lock:
            msgs = self.messages.get(user, ())
            if not msgs: return (0, None, None)
            return (len(msgs), min(m['timestamp'] for m in msgs),
                    max(m['timestamp'] for m in msgs))

    def query_messages(self, user, stale=False):
        with self.lock:
            return self.messages.get(user, [])

    def pop_messages(self, user, stale=False):
        with self.lock:
            return self.messages.pop(user, [])

    def add_message(self, user, message):
        message['id'] = id(message)
        message['to'] = user
        with self.lock:
            self.messages.setdefault(user, []).append(message)

    def query_delivery(self, msgid):
        with self.lock:
            return self.deliveries.get(msgid)

    def add_delivery(self, msg, msgid, timestamp):
        with self.lock:
            msg['delivered_to'] = msgid
            msg['delivered'] = timestamp
            self.deliveries[msgid] = msg

    def gc(self):
        deadline = time.time() - REPLY_TIMEOUT
        with self.lock:
            for k, v in tuple(self.deliveries.items()):
                if v['delivered'] < deadline:
                    del self.deliveries[k]

class NotificationDistributorSQLite(NotificationDistributor):
    def __init__(self, filename):
        self.filename = filename
        self.lock = DBLock(None)
        self.conn = None
        self.curs = None
        self.init()

    def init(self):
        with self.lock.committing:
            self.conn = sqlite3.connect(self.filename, isolation_level='',
                                        check_same_thread=False)
            self.curs = self.conn.cursor()
            self.lock.conn = self.conn
            # Message table.
            self.curs.execute('CREATE TABLE IF NOT EXISTS messages ('
                                  'sender TEXT,'
                                  'recipient TEXT,'
                                  'reason TEXT,'
                                  'text TEXT,'
                                  'timestamp REAL,'
                                  'delivered_to TEXT UNIQUE,'
                                  'delivered REAL'
                              ')')
            # Group table.
            self.curs.execute('CREATE TABLE IF NOT EXISTS groups ('
                                  'groupname TEXT,'
                                  'member TEXT,'
                                  'name TEXT,'
                                  'PRIMARY KEY (groupname, member)'
                              ')')
            # Seen table.
            self.curs.execute('CREATE TABLE IF NOT EXISTS seen ('
                                  'user TEXT PRIMARY KEY,'
                                  'name TEXT,'
                                  'timestamp REAL,'
                                  'unread INTEGER,'
                                  'room TEXT'
                              ')')
            # Alias table.
            self.curs.execute('CREATE TABLE IF NOT EXISTS aliases ('
                                  'base TEXT,'
                                  'user TEXT PRIMARY KEY,'
                                  'name TEXT'
                              ')')
            # Schema upgrades.
            self.curs.execute('PRAGMA table_info(seen);')
            seencols = set(i[1] for i in self.curs.fetchall())
            for coldesc in ('unread INTEGER', 'room TEXT'):
                if coldesc.partition(' ')[0] not in seencols:
                    self.curs.execute('ALTER TABLE seen '
                        'ADD COLUMN ' + coldesc)

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
        ret = self.normalize_user(name)
        with self.lock:
            self.curs.execute('SELECT base FROM aliases WHERE user = ?',
                              (ret[0],))
            res = self.curs.fetchone()
            if res: return (res[0], ret[1])
        return ret

    def query_seen(self, user):
        with self.lock:
            self.curs.execute('SELECT name, timestamp, unread, room '
                'FROM seen WHERE user = ?', (user,))
            return self.curs.fetchone()

    def update_seen(self, user, name, timestamp, unread, room):
        with self.lock.committing:
            self.curs.execute('SELECT unread FROM seen WHERE user = ?',
                              (user,))
            old_unread = self.curs.fetchone()
            if old_unread is None or old_unread[0] is None: old_unread = (0,)
            if unread is None: unread = old_unread[0]
            self.curs.execute('INSERT OR REPLACE INTO seen '
                'VALUES (?, ?, ?, ?, ?)',
                (user, name, timestamp, unread, room))
            return (old_unread[0] != unread)

    def query_aliases(self, base):
        with self.lock:
            self.curs.execute('SELECT user, name FROM aliases '
                'WHERE base = ? ORDER BY _rowid_', (base,))
            return self.curs.fetchall()

    def add_aliases(self, base, names):
        with self.lock.committing:
            qnames, effnames = OrderedSet.firstel(), OrderedSet.firstel()
            for n in names:
                self.curs.execute('SELECT base, user FROM aliases '
                    'WHERE user = ?', (n[0],))
                r = self.curs.fetchone()
                qnames.append(r if r else n)
            for n in names:
                effnames.append(n)
                self.curs.execute('SELECT user, name FROM aliases '
                    'WHERE base = (SELECT base FROM aliases WHERE user = ?) '
                    'ORDER BY _rowid_',
                    (n[0],))
                effnames.extend(self.curs.fetchall())
            self.curs.executemany('INSERT OR REPLACE INTO aliases '
                'VALUES (?, ?, ?)', ((base, m, n) for m, n in effnames))
            for n in qnames:
                self.curs.execute('UPDATE OR IGNORE groups SET member = ? '
                    'WHERE member = ?', (base, n[0]))
                self.curs.execute('DELETE FROM groups WHERE member = ?',
                                  (n[0],))
            entry, unread = None, 0
            for n in qnames:
                self.curs.execute('SELECT name, timestamp, unread, room '
                    'FROM seen WHERE user = ?', (n[0],))
                s = self.curs.fetchone()
                if not s: continue
                unread += s[2]
                if entry is None or s[1] > entry[1]:
                    entry = s
                self.curs.execute('DELETE FROM seen WHERE user = ?', (n[0],))
            if entry:
                self.curs.execute('INSERT INTO seen VALUES (?, ?, ?, ?, ?)',
                    (base, entry[0], entry[1], unread, entry[3]))

    def remove_aliases(self, base, names):
        with self.lock.committing:
            self.curs.executemany('DELETE FROM aliases WHERE base = ? '
                'AND user = ?', ((base, n[0]) for n in names))

    def list_groups(self):
        with self.lock:
            self.curs.execute('SELECT DISTINCT groupname FROM groups')
            return list(i[0] for i in self.curs.fetchall())

    def query_group(self, name):
        with self.lock:
            self.curs.execute('SELECT member, name FROM groups '
                'WHERE groupname = ? ORDER BY _rowid_', (name,))
            return self.curs.fetchall()

    def update_group(self, name, members):
        with self.lock.committing:
            self.curs.execute('DELETE FROM groups WHERE groupname = ?',
                              (name,))
            self.curs.executemany('INSERT INTO groups VALUES (?, ?, ?)',
                                  ((name, m, n) for m, n in members))

    def message_bounds(self, user):
        with self.lock:
            self.curs.execute('SELECT COUNT(*), MIN(timestamp), '
                'MAX(timestamp) FROM messages WHERE recipient = ? '
                'AND delivered IS NULL', (user,))
            return self.curs.fetchone()

    def query_messages(self, user, stale=False):
        with self.lock:
            query = ('SELECT _rowid_, * FROM messages '
                'WHERE recipient = ? %s ORDER BY timestamp') % (
                '' if stale else 'AND delivered IS NULL')
            self.curs.execute(query, (user,))
            return self._unwrap_messages(self.curs.fetchall())

    def pop_messages(self, user, stale=False):
        with self.lock.committing:
            query = ('SELECT _rowid_, sender, reason, text, '
                'timestamp FROM messages WHERE recipient = ? '
                '%s ORDER BY timestamp') % (
                '' if stale else 'AND delivered IS NULL')
            self.curs.execute(query, (user,))
            msgs = tuple(self.curs.fetchall())
            return self._unwrap_messages((i, s, user, w, c, t, None, None)
                                         for i, s, w, c, t in msgs)

    def add_message(self, user, message):
        message['to'] = user
        with self.lock.committing:
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
        with self.lock.committing:
            self.curs.execute('UPDATE messages SET delivered_to = ?, '
                'delivered = ? WHERE _rowid_ = ?', (msgid, timestamp,
                                                    msg['id']))

    def gc(self):
        deadline = time.time() - REPLY_TIMEOUT
        with self.lock.committing:
            self.curs.execute('DELETE FROM messages WHERE delivered < ?',
                              (deadline,))

class TellBot(basebot.Bot):
    BOTNAME = 'TellBot'
    NICKNAME = 'TellBot'
    SHORT_HELP = 'I can schedule messages to be delivered to other users.'
    LONG_HELP = HELP_TEXT

    def _format_nick(self, nick, ping=True, subject=None, title=False):
        nnick = basebot.normalize_nick(nick)
        ttr = (titlefirst if title else lambda x: x)
        if subject and nnick == basebot.normalize_nick(subject):
            return ttr('yourself')
        elif nnick == basebot.normalize_nick(self.nickname):
            return ttr('myself')
        else:
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
        basebot.Bot.handle_chat_ex(self, msg, meta)
        distr, reply = self.manager.distributor, meta['reply']
        user, now = distr.query_user(msg['sender']['name']), time.time()

        # Update online time database.
        if meta['edit'] or meta['long']: return
        unread, oldest, newest = distr.message_bounds(user[0])
        update = distr.update_seen(user[0], user[1], now, unread,
                                   self.roomname)

        # Deliver messages to myself.
        if msg['sender']['session_id'] == self.session_id:
            messages = distr.pop_messages(user[0])
            for m in messages:
                distr.add_delivery(m, None, now)
                # ... reading ...
            if len(messages) == 1:
                reply('/me read 1 message.')
            elif messages:
                reply('/me read %s messages.' % len(messages))
        elif update:
            if re.match(r'!(inbox|boop)\b', msg['content']):
                pass
            elif oldest is not None and oldest >= now - 86400: # 1 day
                self.deliver_notifies(distr, user, reply, False)
            elif unread == 1:
                reply('You have 1 unread message; use !inbox to read it.')
            elif unread > 1:
                reply('You have %s unread messages; use !inbox to read '
                      'them.' % unread)

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
            distr.add_message(user, dict(base, tonick=nick,
                                         reason=cur_reason))

        # Reply.
        reply('Will tell %s.' % reclist)

    def deliver_notifies(self, distr, sender, reply, stale=False):
        # Format a delivery reason.
        def format_reason(src):
            if src.startswith('<re> '):
                res = format_reason(src[5:])
                return ' replying' + res
            elif src.startswith('@'):
                return ' to ' + self._format_nick(src[1:], False, sender[1])
            else:
                return ' to ' + src

        # Actually deliver a message.
        def deliver_message():
            # Add a delivery notice.
            def handle_delivery(reply):
                distr.add_delivery(m, reply.data.id, reply.data.time)
                deliver_message()
            try:
                m = next(msgitr)
            except StopIteration:
                return
            distr.add_delivery(m, None, now)
            if m['reason'] == make_mention(sender[1]):
                reason = ''
            else:
                reason = format_reason(m['reason'])
            reply('[%s%s, %s ago] %s' % (
                self._format_nick(m['from'], False, sender[1], True),
                reason,
                basebot.format_delta(now - m['timestamp'], False),
                m['text']), handle_delivery)

        # Deliver messages.
        now = time.time()
        messages = distr.pop_messages(sender[0], stale)
        msgitr = iter(messages)
        deliver_message()
        distr.update_seen(sender[0], sender[1], now, 0,
                          self.roomname)

        # ...Or none.
        if not messages:
            reply('No mail.')

    def handle_command(self, cmdline, meta):
        # Common part of the argument parsers.
        def parse_userlist(base, groups, it, grouppol='normal'):
            def groupfirst():
                reply('Please specify a group first.')
                return Ellipsis, count
            def nogroups():
                reply('Please do not specify groups.')
                return Ellipsis, count
            count = 0
            for arg in it:
                if arg.startswith('@'): # Add user.
                    if grouppol == 'get': return groupfirst()
                    u = distr.query_user(arg[1:])
                    base.append(u)
                    groups[arg] = [u]
                    count += 1
                elif arg.startswith('*'): # Add group.
                    if grouppol == 'get': return arg, count
                    elif grouppol == 'none': return nogroups()
                    g = distr.query_group(arg[1:])
                    base.extend(g)
                    groups[arg] = g
                    count += 1
                elif arg.startswith('+@'): # Add user (long form).
                    if grouppol == 'get': return groupfirst()
                    u = distr.query_user(arg[2:])
                    base.append(u)
                    groups[arg[1:]] = [u]
                    count += 1
                elif arg.startswith('+*'): # Add group (long form).
                    if grouppol == 'get': return groupfirst()
                    elif grouppol == 'none': return nogroups()
                    g = distr.query_group(arg[2:])
                    base.extend(g)
                    groups[arg[1:]] = g
                    count += 1
                elif arg.startswith('-@'): # Discard user.
                    if grouppol == 'get': return groupfirst()
                    base.discard(distr.query_user(arg[2:]))
                    count += 1
                elif arg.startswith('-*'): # Discard group.
                    if grouppol == 'get': return groupfirst()
                    elif grouppol == 'none': return nogroups()
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
        def format_nick(item, ping, title=False):
            return self._format_nick(item[1], ping, sender[1], title)

        # Reply with the users from a given list.
        def display_group(groupname, members, ping, comment):
            head = 'Members of *%s%s%s%s: ' % (groupname,
                ' ' if comment else '', comment,
                ' (%s)' % len(members) if members else '')
            tr = lambda x: format_nick(x, ping)
            lst = format_list(map(tr, members), '-none-')
            reply(head + lst)

        # Accumulate a reply.
        def reply(msg):
            replybuf.append(msg)
        # Drain all replies.
        def flush(msg=None):
            if msg is not None:
                replybuf.append(msg)
            if replybuf:
                meta['reply']('\n'.join(replybuf))
                replybuf[:] = []

        basebot.Bot.handle_command(self, cmdline, meta)
        distr = self.manager.distributor
        sender = distr.query_user(meta['sender'])
        replybuf = []

        # Ensure replies are delivered.
        try:

            # Send a message.
            if cmdline[0] in ('!tell', '!tnotify'):
                self._log_command(cmdline)
                # Parse arguments.
                recipients = OrderedSet.firstel()
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
                self.send_notify(distr, sender, recipients, groups, text,
                                 reply)

            # Reply to a freshly delivered message.
            elif cmdline[0] == '!reply':
                self._log_command(cmdline)
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
                    OrderedSet.firstel((recipient,)),
                    {'@' + recipient[0]: [recipient]},
                    meta['line'][cmdline[1].offset:],
                    reply,
                    '<re> ' + make_mention(recipient[1]))

            # Reply to a group.
            elif cmdline[0] == '!reply-all':
                self._log_command(cmdline)
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
                recipients = OrderedSet.firstel(groups[reason])

                # Send message.
                self.send_notify(distr, sender, recipients, groups,
                    meta['line'][cmdline[1].offset:], reply,
                    '<re> ' + reason)

            # Enumerate available groups.
            elif cmdline[0] == '!tgrouplist':
                self._log_command(cmdline)
                # Parse arguments.
                if len(cmdline) == 1:
                    filt = lambda x: True
                    filt_all = True
                elif len(cmdline) == 2:
                    regex = re.compile(fnmatch.translate(cmdline[1]), re.I)
                    filt = regex.match
                    filt_all = False
                else:
                    reply('Please specify a matching pattern or nothing.')
                    return

                # Obtain list.
                names = ['*' + i for i in distr.list_groups() if filt(i)]
                names.sort(key=lambda x: x.lower())

                if not names:
                    reply('No groups.' if filt_all else
                          'No groups mathing pattern.')
                    return

                # Group by first character.
                groups = []
                for n in names:
                    if not groups:
                        groups.append([n])
                    elif n[:2].lower() != groups[-1][-1][:2].lower():
                        groups.append([n])
                    else:
                        groups[-1].append(n)

                # Output.
                reply('\n'.join(map(', '.join, groups)))

            # Update a group.
            elif cmdline[0] in ('!tgroup', '!tungroup'):
                self._log_command(cmdline)
                # Parse arguments.
                groupname, members, groups, ping = None, None, None, False
                it, count = iter(cmdline[1:]), 0
                while 1:
                    arg, cnt = parse_userlist(members, groups, it,
                        ('get' if groupname is None else 'normal'))
                    count += cnt
                    if arg is None:
                        break
                    elif arg is Ellipsis:
                        return
                    elif arg.startswith('*'):
                        groupname = arg[1:]
                        old_members = distr.query_group(groupname)
                        if cmdline[0] == '!tgroup':
                            members = OrderedSet.firstel(old_members)
                        else:
                            members = OrderedSet.firstel()
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
                elif cmdline[0] == '!tungroup' and count == 0:
                    reply('Nothing to be done.')
                    return

                # Display old membership.
                display_group(groupname, old_members, ping,
                              '' if count == 0 else 'before')
                if count == 0: return

                # Apply changes.
                if cmdline[0] == '!tungroup':
                    removes = members
                    members = OrderedSet.firstel(old_members)
                    members.discard_all(removes)
                distr.update_group(groupname, tuple(members))

                # Display new membership.
                display_group(groupname, members, ping, 'after')

            # When was a user last active?
            elif cmdline[0] == '!seen':
                self._log_command(cmdline)
                # Parse arguments.
                users, groups = OrderedSet.firstel(), {}
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
                    if seen is None: seen = (None, None, 0, None)
                    unread, oldest, newest = distr.message_bounds(user)
                    if not unread:
                        pm = ''
                    elif unread == 1:
                        pm = ' (1 pending message)'
                    else:
                        pm = ' (%s pending messages)' % unread
                    fnick = titlefirst(format_nick((user, nick), True))
                    if seen[1] is None:
                        reply('%s not seen%s.' % (fnick, pm))
                        continue
                    if bnn(nick) != bnn(seen[0]):
                        comment = ' (as %s)' % format_nick((user, seen[0]),
                                                           True)
                    else:
                        comment = ''
                    if seen[3] is None:
                        room = ''
                    elif seen[3] == self.roomname:
                        room = ' here'
                    else:
                        room = ' in &' + seen[3]
                    if now - seen[1] < 1:
                        delta = 'just now'
                    else:
                        delta = (basebot.format_delta(now - seen[1], False) +
                                 ' ago')
                    reply('%s%s last seen%s on %s, %s%s.' % (fnick, comment,
                        room, basebot.format_datetime(seen[1], False), delta,
                        pm))

            # Deliver pending messages.
            elif cmdline[0] in ('!inbox', '!boop'):
                self._log_command(cmdline)
                # Parse arguments
                stale = False
                for arg in cmdline[1:]:
                    if arg == '--stale':
                        stale = True
                    elif arg == '--':
                        break
                    elif arg.startswith('-'):
                        reply('Unknown option %r.' % arg)

                # Deliver messages.
                self.deliver_notifies(distr, sender, meta['reply'], stale)

        # Deliver replies.
        finally:
            flush()

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
            wakeup = time.time() + GC_INTERVAL
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
