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
