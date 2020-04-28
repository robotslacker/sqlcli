import os
import re
import locale
import logging
import subprocess
from io import open

import click

from . import export
from .main import special_command

use_expanded_output = False
PAGER_ENABLED = True
tee_file = None
once_file = written_to_once_file = None


@export
def set_expanded_output(val):
    global use_expanded_output
    use_expanded_output = val


@export
def is_expanded_output():
    return use_expanded_output


_logger = logging.getLogger(__name__)

@special_command(
    "tee",
    "tee [-o] filename",
    "Append all results to an output file (overwrite using -o).",
)
def set_tee(arg, **_):
    global tee_file

    try:
        tee_file = open(**parseargfile(arg))
    except (IOError, OSError) as e:
        raise OSError("Cannot write to file '{}': {}".format(e.filename, e.strerror))

    return [(None, None, None, "")]


@export
def close_tee():
    global tee_file
    if tee_file:
        tee_file.close()
        tee_file = None


@special_command("notee", "notee", "Stop writing results to an output file.")
def no_tee(arg, **_):
    close_tee()
    return [(None, None, None, "")]


@export
def write_tee(output):
    global tee_file
    if tee_file:
        click.echo(output, file=tee_file, nl=False)
        click.echo("\n", file=tee_file, nl=False)
        tee_file.flush()


@special_command(
    ".once",
    "\\o [-o] filename",
    "Append next result to an output file (overwrite using -o).",
    aliases=("\\o", "\\once"),
)
def set_once(arg, **_):
    global once_file

    once_file = parseargfile(arg)

    return [(None, None, None, "")]


@export
def write_once(output):
    global once_file, written_to_once_file
    if output and once_file:
        try:
            f = open(**once_file)
        except (IOError, OSError) as e:
            once_file = None
            raise OSError(
                "Cannot write to file '{}': {}".format(e.filename, e.strerror)
            )

        with f:
            click.echo(output, file=f, nl=False)
            click.echo("\n", file=f, nl=False)
        written_to_once_file = True


@export
def unset_once_if_written():
    """Unset the once file, if it has been written to."""
    global once_file
    if written_to_once_file:
        once_file = None

