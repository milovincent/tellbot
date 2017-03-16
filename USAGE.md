# tellbot usage

`@TellBot` is in a public alpha release, and under active development.
The command set may change upon popular demand or developer decision.

## !tell a.k.a. !tnotify

    !tell <user-list> [--] <message>
    !tnotify <user-list> [--] <message>

Schedule `message` to be delivered to all users in the given
[`user-list`](#user-lists). A `--` separator may be used to separate the
recipients from the message body (such as when the message starts with an
@-mention; it is not included in the delivered message. (To send a message
starting with a double dash, duplicate the separator.)

If you submit a notify to a group that does include yourself and do not
explicitly include yourself as a recipient, you will be dropped from the
recipient list; this aids messaging groups. Explicit mention of yourself
is interpreted as the explicit intention of delivering the message to
yourself.

`!tell` and `!tnotify` are exactly equivalent; the latter is provided for
closeness to the corresponding `@NotBot` command.

**Examples**

    !tell @person1 something

    !tnotify @person2 @person3 something else

    !tell *group -- @somebot stopped working, can you check?

## !reply and !reply-all

    !reply <message>
    !reply-all <message>

If (and only if) used as direct replies to delivered messages that are not
older than some implementation-defined time (_i.e._ one hour), these commands
will send a message back to the sender of the received message (`!reply`) or
the group the message was sent to (`!reply-all`). If the message was not sent
to a group, both behave equivalently.

The message starts immediately after the command (and can in particular start
with any character). Implicit self exclusion necessarily happens in the case
of `!reply-all`, and does not happen for `!reply`.

**Examples**

    [From yourself to yourself, 5s ago] some message
      !reply some reply

    [From @person to *group, 5s ago] another message
      !reply Can you clarify that a bit?

    [From @person to *group, 5s ago] yet another message
      !reply-all another reply

## !tgroup

    !tgroup [--ping] *<group> [<user-list>]

Update the given `group` with the result of building
[`user-list`](#user-lists) basing upon it. If `user-list` is empty, the
members of the group are displayed without mutating it. Unless `--ping` is
passed, user names are not @-mentioned to avoid unnecessary alerting.

**Examples**

    !tgroup *group @person1 @person2
      Members of *group before: -none-
      Members of *group after: person1, person2

    !tgroup --ping *group
      Members of *group: @person1, @person2

    !tgroup *group -*group
      Members of *group before: person1, person2
      Members of *group after: -none-

## !tgrouplist

    !tgrouplist [pattern]

Enumerate all groups (or those whose name without the `*` sigil match a
globbing `pattern`) known to `@TellBot`. The output is alphabetically
sorted.

`pattern` may not contain whitespace, and can include the following
metacharacters:

-   `?` matches an arbitrary single character.

    To match a literal question mark, enclose it in a character class (_i.e._
    use `[?]`; see below).

-   `*` matches an arbitrary amount of arbitrary characters (including none).

    To match a literal asterisk, enclose it — similarly to the question
    mark — in a character class (_i.e._ use `[*]`).

-   `[` initiates a _character class_ that matches one character of any such
    mentioned in it; it is closed by a `]`, and the `[` may be immediately
    followed by `!` denoting a negative match; character ranges (separated by
    dashes `-`) are allowed.

    To include a closing bracket (`]`), place it immediately after the
    opening bracket (`[`) or the negation sign (`!`); to include a dash,
    place it at the beginning (similarly to `]`) or end (immediately before
    the closing `]`) of the character class. Nested opening brackets are not
    treated specially.

    Thus, `[][]` matches a closing or an opening bracket; `[!]]` matches
    anything but a closing bracket; `[?-]` matches a question mark or a
    hyphen; `[a-z]` matches a letter of (see below) any case.

The pattern must match the entire group name (ignoring case); to "de-anchor"
it from an end, use leading or trailing asterisks `*`.

**Examples**

    !tgrouplist
      *anyquestions?
      *botdevs
      *groupA, *groupB, *groupC, *groupD
      *test, *testing

    !tgrouplist group?
      *groupA, *groupB, *groupC, *groupD

    !tgrouplist *st*
      *anyquestions?
      *test, *testing

    !tgrouplist [gt]*
      *groupA, *groupB, *groupC, *groupD
      *test, *testing

    !tgrouplist *[c-g]
      *groupC, *groupD
      *testing

    !tgrouplist *[?]
      *anyquestions?

## !seen

    !seen <user-list>

Reply with time intervals since the users in [`user-list`](#user-lists) were
last seen (_i.e._ posted something in a room observed by `@TellBot` whilst it
was running).

**Examples**

    !seen @person1
      @person1 last seen 5m 2s ago.

    !seen *group
      @person2 last seen 1d 4h 5s ago.
      @person3 last seen 41d 23h 59m ago.
