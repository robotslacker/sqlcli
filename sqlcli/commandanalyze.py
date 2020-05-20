# -*- coding: utf-8 -*-
import logging
from collections import namedtuple

log = logging.getLogger(__name__)

NO_QUERY = 0
PARSED_QUERY = 1
RAW_QUERY = 2

__all__ = []


def export(defn):
    """Decorator to explicitly mark functions that are exposed in a lib."""
    globals()[defn.__name__] = defn
    __all__.append(defn.__name__)
    return defn


SpecialCommand = namedtuple(
    "SpecialCommand",
    [
        "handler",
        "command",
        "description",
        "arg_type",
        "hidden",
        "case_sensitive",
    ],
)

COMMANDS = {}


@export
class ArgumentMissing(Exception):
    pass


@export
class CommandNotFound(Exception):
    pass


@export
def parse_special_command(sql):
    command, _, arg = sql.partition(" ")
    verbose = "+" in command
    command = command.strip().replace("+", "")
    return command, verbose, arg.strip()


@export
def special_command(
    command,
    description,
    arg_type=PARSED_QUERY,
    hidden=False,                     # 是否显示在帮助信息里头
    case_sensitive=False,             # 是否忽略输入的大小写
):
    def wrapper(wrapped):
        register_special_command(
            wrapped,
            command,
            description,
            arg_type,
            hidden,
            case_sensitive,
        )
        return wrapped

    return wrapper


@export
def register_special_command(
    handler,
    command,
    description,
    arg_type=PARSED_QUERY,
    hidden=False,
    case_sensitive=False
):
    cmd = command.lower() if not case_sensitive else command
    COMMANDS[cmd] = SpecialCommand(
        handler, command, description, arg_type, hidden, case_sensitive
    )


@export
def execute(cur, sql):
    """Execute a special command and return the results. If the special command
    is not supported a KeyError will be raised.
    """

    command, verbose, arg = parse_special_command(sql)

    if (command not in COMMANDS) and (command.lower() not in COMMANDS):
        raise CommandNotFound

    try:
        special_cmd = COMMANDS[command]
    except KeyError:
        special_cmd = COMMANDS[command.lower()]
        if special_cmd.case_sensitive:
            raise CommandNotFound("Command not found: %s" % command)

    if special_cmd.arg_type == NO_QUERY:
        return special_cmd.handler()
    elif special_cmd.arg_type == PARSED_QUERY:
        return special_cmd.handler(cur=cur, arg=arg, verbose=verbose)
    elif special_cmd.arg_type == RAW_QUERY:
        return special_cmd.handler(cur=cur, query=sql)


@special_command(
    "help", "Show this help.", arg_type=NO_QUERY
)
def show_help():  # All the parameters are ignored.
    headers = ["Command", "Description"]
    result = []

    for _, value in sorted(COMMANDS.items()):
        if not value.hidden:
            result.append((value.command, value.description))
    return [(None, result, headers, None)]


@special_command("quit", "Quit.", arg_type=NO_QUERY)
def quit_sqlcli(*_args):
    raise EOFError
