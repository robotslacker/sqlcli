# -*- coding: utf-8 -*-
from collections import namedtuple

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
        "hidden",
        "case_sensitive",
    ],
)

COMMANDS = {}


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
    hidden=False,                     # 是否显示在帮助信息里头
    case_sensitive=False,             # 是否忽略输入的大小写
):
    def wrapper(wrapped):
        register_special_command(
            wrapped,
            command,
            description,
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
    hidden=False,
    case_sensitive=False
):
    cmd = command.lower() if not case_sensitive else command
    COMMANDS[cmd] = SpecialCommand(
        handler, command, description, hidden, case_sensitive
    )


@export
def execute(sql):
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

    return special_cmd.handler(arg=arg)


@special_command("help", "Show this help.")
def show_help(arg):
    headers = ["Command", "Description"]
    result = []

    for _, value in sorted(COMMANDS.items()):
        if not value.hidden:
            for m_desc in value.description.split('\n'):
                result.append((value.command, m_desc))
    return [(None, result, headers, None, None)]


@special_command("quit", "Quit.")
def quit_sqlcli(arg):
    raise EOFError
