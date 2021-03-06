#!/usr/bin/env python3
# -*- coding: ascii -*-

import json
import optparse

import tellbot

# @NotBot data dump format, as approved in a behind-the-curtains discussion
# by the original developer.
#
# Message file format:
# - Serialized using JSON
# - The top-level objects maps user names (using lowercase; without
#   whitespace; without the leading @ sign) to message arrays
# - Message arrays are arrays of message descriptions
# - Message descriptions are arrays with four fixed elements each:
#   - The original recipient of the message as a string (a user name or a
#     group name; with leading @ sign or asterisk present; using original
#     case; without whitespace)
#   - The sender of the message as a string (using original case; without
#     whitespace; without the leading @ sign)
#   - The content of the message as a string (after normalizing all runs of
#     whitespace to single spaces and stripping leading and trailing spaces)
#   - The UNIX timestamp of the submission of the message as a number
# - Message arrays may be empty
#
# Group file format:
# - Serialized using JSON
# - The top-level object maps group names (using original case; without the
#   leading asterisk) to group contents
# - Group contents are arrays of strings containing nicknames (using original
#   case; without whitespace; without the leading @ sign)
# - Groups may be empty

def import_messages(f, distr, seen):
    with distr.lock.committing:
        seendat = {}
        for recipient, messages in json.load(f).items():
            for item in messages:
                msg = {'from': item[1], 'to': recipient, 'reason': item[0],
                       'text': item[2], 'timestamp': item[3],
                       'priority': 'NORMAL'}
                distr.add_message(recipient, msg)
                if seen:
                    normsender = distr.normalize_user(item[1])
                    old_ent = seendat.get(normsender[0])
                    if old_ent is None or old_ent[1] < item[3]:
                        seendat[normsender[0]] = (normsender[1], item[3])
        for user, entry in seendat.items():
            old_seen = distr.query_seen(user)
            if old_seen is None:
                distr.update_seen(user, entry[0], entry[1], 0, None)
            elif old_seen[1] < entry[1]:
                distr.update_seen(user, entry[0], entry[1], old_seen[2],
                                  old_seen[3])

def import_groups(f, distr):
    with distr.lock.committing:
        for name, members in json.load(f).items():
            if not members: continue
            old_members = distr.query_group(name)
            entries = tellbot.OrderedSet.firstel(old_members)
            entries.extend(distr.normalize_user(m) for m in members)
            distr.update_group(name, list(entries))

def main():
    parser = optparse.OptionParser(usage='%prog [-h|--help] '
            '[--messages=path] [--seen] [--groups=path] msgdb',
        description='Import data from @NotBot into a @TellBot database.\n'
            'WARNING: The operation is NOT idempotent; importing the same '
            'messages twice will cause duplication.',
        epilog='msgdb is the path of an SQLite database used by @TellBot. '
            'Live updates are supported.')
    parser.add_option('--messages', action='append', dest='messages',
                      metavar='path', default=[],
                      help='read messages from JSON file (may be repeated)')
    parser.add_option('--seen', action='store_true', dest='seen',
                      default=False, help='harvest seen data from messages '
                      'being imported')
    parser.add_option('--groups', action='append', dest='groups',
                      metavar='path', default=[],
                      help='read groups from JSON file (may be repeated)')
    options, args = parser.parse_args()
    if len(args) < 1:
        parser.error('missing message database')
    elif len(args) > 1:
        parser.error('excess command line arguments')
    dbpath = args[0]
    distr = tellbot.NotificationDistributorSQLite(dbpath)
    for p in options.messages:
        with open(p) as f:
            import_messages(f, distr, options.seen)
    for p in options.groups:
        with open(p) as f:
            import_groups(f, distr)

if __name__ == '__main__': main()
