# -*- coding: utf-8 -*-
import os
import sys
import traceback
import logging
from time import time
from datetime import datetime
from io import open
import jaydebeapi
import re

from cli_helpers.tabular_output import TabularOutputFormatter
from cli_helpers.tabular_output import preprocessors
import click

from prompt_toolkit.shortcuts import PromptSession
from .packages import special
from .clistyle import style_factory_output
from .sqlexecute import SQLExecute
from .sqlinternal import Create_file
from .config import config_location, get_config
from .__init__ import __version__

import itertools

click.disable_unicode_literals_warning = True

PACKAGE_ROOT = os.path.abspath(os.path.dirname(__file__))


class SQLCli(object):
    default_prompt = "SQL> "
    max_len_prompt = 45

    jar_file = None
    driver_class = None
    db_url = None
    db_username = None
    db_password = None
    db_type = None
    db_driver_type = None
    db_host = None
    db_port = None
    db_service_name = None
    db_conn = None
    sqlscript = None
    sqlexecute = None
    nologo = None

    def __init__(
            self,
            logfile=None,
            auto_vertical_output=False,
            sqlclirc=None,
            sqlscript=None,
            nologo=None
    ):
        self.sqlexecute = SQLExecute()
        __sqlinternal__sqlexecute__ = self.sqlexecute
        print(str(__sqlinternal__sqlexecute__))
        self.logfile = logfile
        self.sqlscript = sqlscript
        self.nologo = nologo

        # Load config.
        c = self.config = get_config(sqlclirc)

        self.formatter = TabularOutputFormatter(format_name=c["main"]["table_format"])
        self.formatter.sqlcli = self
        self.syntax_style = c["main"]["syntax_style"]
        self.cli_style = c["colors"]
        self.output_style = style_factory_output(self.syntax_style, self.cli_style)

        # read from cli argument or user config file
        self.auto_vertical_output = auto_vertical_output or c["main"].as_bool(
            "auto_vertical_output"
        )

        # 打开日志
        self.logger = logging.getLogger(__name__)

        # 处理一些特殊的命令
        self.register_special_commands()

        self.prompt_app = None

    def register_special_commands(self):

        # 加载数据库驱动
        special.register_special_command(
            self.load_driver,
            ".load",
            ".load",
            "load JDBC driver .",
            aliases=("load", "\\l"),
        )

        # 连接数据库
        special.register_special_command(
            self.connect_db,
            ".connect",
            ".connect",
            "Connect to database .",
            aliases=("connect", "\\c"),
        )

        special.register_special_command(
            self.change_table_format,
            ".mode",
            "\\T",
            "Change the table format used to output results.",
            aliases=("tableformat", "\\T"),
            case_sensitive=True,
        )

        # 从文件中执行脚本
        special.register_special_command(
            self.execute_from_file,
            "start",
            "\\. filename",
            "Execute commands from file.",
            aliases=("\\.",),
        )

        # 设置各种参数选项
        special.register_special_command(
            self.set_options,
            "set",
            "set parameter parameter_value",
            "set options .",
            aliases=("set", "\\c"),
        )

        # 执行特殊的命令
        special.register_special_command(
            self.exeucte_internal_command,
            "__internal__",
            "execute internal command.",
            "execute internal command.",
            aliases=("__internal__",),
        )

    def change_table_format(self, arg, **_):
        try:
            self.formatter.format_name = arg
            yield None, None, None, "Changed table format to {}".format(arg)
        except ValueError:
            msg = "Table format {} not recognized. Allowed formats:".format(arg)
            for table_type in self.formatter.supported_formats:
                msg += "\n\t{}".format(table_type)
            yield None, None, None, msg

    # 加载JDBC驱动文件
    def load_driver(self, arg, **_):
        if arg is None:
            raise Exception("Missing required argument, load [driver file name] [driver class name].")
        elif arg == "":
            raise Exception("Missing required argument, load [driver file name] [driver class name].")
        else:
            load_parameters = str(arg).split();
            if len(load_parameters) != 2:
                self.logger.debug('arg = ' + str(load_parameters))
                raise Exception("Missing required argument, load [driver file name] [driver class name].")
            if not os.path.exists(str(load_parameters[0])):
                raise Exception("driver file [" + str(arg) + "] does not exist.")

            # 如果jar包或者驱动类发生了变化，则当前数据库连接自动失效
            if self.jar_file:
                if self.jar_file != str(load_parameters[0]):
                    self.db_conn = None
                    self.sqlexecute.set_connection(None)
            if self.driver_class:
                if self.driver_class != str(load_parameters[1]):
                    self.db_conn = None
                    self.sqlexecute.set_connection(None)
            self.jar_file = str(load_parameters[0])
            self.driver_class = str(load_parameters[1])

        yield (
            None,
            None,
            None,
            'Driver loaded.'
        )

    # 连接数据库
    def connect_db(self, arg, **_):
        if arg is None:
            raise Exception("Missing required argument\n." + "connect [user name]/[password]@" +
                            "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
        elif arg == "":
            raise Exception("Missing required argument\n." + "connect [user name]/[password]@" +
                            "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
        elif self.jar_file is None:
            raise Exception("Please load driver first.")

        # 去掉为空的元素
        connect_parameters = [var for var in re.split(r'//|:|@|/', arg) if var]
        if len(connect_parameters) == 8:
            # 指定了所有的数据库连接参数
            self.db_username = connect_parameters[0]
            self.db_password = connect_parameters[1]
            self.db_type = connect_parameters[3]
            self.db_driver_type = connect_parameters[4]
            self.db_host = connect_parameters[5]
            self.db_port = connect_parameters[6]
            self.db_service_name = connect_parameters[7]
            self.db_url = connect_parameters[2] + ':' + connect_parameters[3] + ':' + \
                          connect_parameters[4] + '://' + connect_parameters[5] + ':' + \
                          connect_parameters[6] + ':/' + connect_parameters[7]
        elif len(connect_parameters) == 2:
            # 用户只指定了用户名和口令， 认为用户和上次保留一直的连接字符串信息
            self.db_username = connect_parameters[0]
            self.db_password = connect_parameters[1]
            if not self.db_url:
                if "SQLCLI_CONNECTION_URL" in os.environ:
                    # 从环境变量里头拼的连接字符串
                    connect_parameters = [var for var in re.split(r'//|:|@|/', os.environ['SQLCLI_CONNECTION_URL']) if
                                          var]
                    if len(connect_parameters) == 6:
                        self.db_type = connect_parameters[1]
                        self.db_driver_type = connect_parameters[2]
                        self.db_host = connect_parameters[3]
                        self.db_port = connect_parameters[4]
                        self.db_service_name = connect_parameters[5]
                        self.db_url = connect_parameters[0] + ':' + connect_parameters[1] + ':' + \
                                      connect_parameters[2] + '://' + connect_parameters[3] + ':' + \
                                      connect_parameters[4] + ':/' + connect_parameters[5]
                    else:
                        print(str(connect_parameters))
                        print(len(connect_parameters))
                        raise Exception("Unexpeced env SQLCLI_CONNECTION_URL\n." +
                                        "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
                else:
                    # 用户第一次连接，而且没有指定环境变量
                    raise Exception("Missing required argument\n." + "connect [user name]/[password]@" +
                                    "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")

        # 连接数据库
        try:
            self.db_conn = jaydebeapi.connect(self.driver_class,
                                              'jdbc:' + self.db_type + ":" + self.db_driver_type + "://" +
                                              self.db_host + ":" + self.db_port + "/" + self.db_service_name,
                                              [self.db_username, self.db_password],
                                              self.jar_file, )
        except Exception as e:  # Connecting to a database fail.
            self.logger.debug("Database connection failed: %r.", e)
            self.logger.error("traceback: %r", traceback.format_exc())
            self.echo(str(e), err=True, fg="red")
            exit(1)
        self.sqlexecute.set_connection(self.db_conn)

        yield (
            None,
            None,
            None,
            'Database connected.'
        )

    # 从文件中执行SQL
    def execute_from_file(self, arg, **_):
        if not arg:
            message = "Missing required argument, filename."
            return [(None, None, None, message)]
        try:
            with open(os.path.expanduser(arg), encoding="utf-8") as f:
                query = f.read()
        except IOError as e:
            return [(None, None, None, str(e))]
        return self.sqlexecute.run(query)

    # 设置一些选项
    def set_options(self, arg, **_):
        b_Successful = False
        if arg is None:
            raise Exception("Missing required argument. set parameter parameter_value.")
        elif arg == "":
            m_Result = []
            for key, value in self.sqlexecute.options.items():
                m_Result.append([str(key), str(value)])
            yield (
                "Current set options: ",
                m_Result,
                ["option","value"],
                ""
            )
        else:
            options_parameters = str(arg).split()
            if len(options_parameters) != 2:
                raise Exception("Missing required argument. set parameter parameter_value.")
            self.sqlexecute.options[options_parameters[0].upper()] = options_parameters[1].upper()
            b_Successful = True

            if b_Successful:
                yield (
                    None,
                    None,
                    None,
                    '')
            else:
                yield (
                    None,
                    None,
                    None,
                    'Invalid Option. Please double check.')

    # 执行特殊的命令
    def exeucte_internal_command(self, arg, **_):
        b_Successful = False
        # 去掉回车换行符，以及末尾的空格
        strSQL = str(arg).replace('\r', '').replace('\n', '').strip()

        matchObj = re.match(r"create\s+file\s+(.*?)\((.*)\)\s+?rows\s+([1-9]\d*)$", strSQL, re.IGNORECASE)
        if matchObj:
            # create file command  将根据格式要求创建需要的文件
            b_Successful = Create_file(p_filename=str(matchObj.group(1)),
                                       p_formula_str=str(matchObj.group(2)),
                                       p_rows=int(matchObj.group(3)),
                                       p_options=self.sqlexecute.options)
        if b_Successful:
            yield (
                None,
                None,
                None,
                str(matchObj.group(3)) + ' rows created Successful.')
        else:
            yield (
                None,
                None,
                None,
                'Invalid internal Command. Please double check.')

    def read_my_cnf_files(self, keys):
        """
        Reads a list of config files and merges them. The last one will win.
        :param files: list of files to read
        :param keys: list of keys to retrieve
        :returns: tuple, with None for missing keys.
        """
        cnf = self.config

        sections = ["main"]

        def get(key):
            result = None
            for sect in cnf:
                if sect in sections and key in cnf[sect]:
                    result = cnf[sect][key]
            return result

        return {x: get(x) for x in keys}

    def run_cli(self):
        iterations = 0
        self.configure_pager()

        if not self.nologo:
            print("SQL*Cli Release " + __version__)

        def one_iteration(text=None):
            # 判断传入SQL语句
            if text is None:
                full_text = None
                while True:
                    try:
                        if full_text is None:
                            text = self.prompt_app.prompt('SQL> ')
                        else:
                            text = self.prompt_app.prompt('   > ')
                    except KeyboardInterrupt:
                        return
                    if full_text is None:
                        full_text = text
                    else:
                        full_text = full_text + '\n' + text
                    if full_text.startswith('set '):
                        break
                    # 如果语句以create|drop|alter|select|update|insert|delete开头，可能出现换行
                    # 如果语句以create|drop|alter开头，且其中包含function, procedure的，可能出现程序段落
                    # print('FULL_TEXT = [' + full_text + ']')
                    if (full_text.startswith('create')
                            or full_text.startswith('drop')
                            or full_text.startswith('alter')
                            or full_text.startswith('select')
                            or full_text.startswith('update')
                            or full_text.startswith('insert')
                            or full_text.startswith('delete')):
                        if full_text.endswith(';'):
                            break
                    else:
                        break
                text = full_text

                special.iocommands.set_expanded_output(False)

            if not text.strip():
                return

            try:
                if self.logfile:
                    self.logfile.write("\n# %s\n" % datetime.now())
                    self.logfile.write(text)
                    self.logfile.write("\n")

                start = time()

                res = self.sqlexecute.run(text)

                self.formatter.query = text
                result_count = 0
                for title, cur, headers, status in res:
                    threshold = 1000
                    if is_select(status) and cur and cur.rowcount > threshold:
                        self.echo(
                            "The result set has more than {} rows.".format(threshold),
                            fg="red",
                        )

                    if self.auto_vertical_output:
                        max_width = self.prompt_app.output.get_size().columns
                    else:
                        max_width = None

                    formatted = self.format_output(
                        title, cur, headers, special.is_expanded_output(), max_width
                    )

                    t = time() - start
                    try:
                        if result_count > 0:
                            self.echo("")
                        try:
                            self.output(formatted, status)
                        except KeyboardInterrupt:
                            pass
                    except KeyboardInterrupt:
                        pass

                    start = time()
                    result_count += 1
                special.unset_once_if_written()
            except EOFError as e:
                raise e
            except NotImplementedError:
                self.echo("Not Yet Implemented.", fg="yellow")
            except Exception as e:
                self.echo(str(e), err=True, fg="red")

        self.prompt_app = PromptSession()

        try:
            # 如果环境变量中包含了SQLCLI_CONNECTION_JAR_NAME或者SQLCLI_CONNECTION_CLASS_NAME
            # 则直接加载
            if "SQLCLI_CONNECTION_JAR_NAME" in os.environ and \
                    "SQLCLI_CONNECTION_CLASS_NAME" in os.environ:
                one_iteration('load ' +
                              os.environ["SQLCLI_CONNECTION_JAR_NAME"] + ' ' + os.environ[
                                  "SQLCLI_CONNECTION_CLASS_NAME"])
                iterations += 1

            # 如果传递的参数中有SQL文件，先执行SQL文件
            if self.sqlscript:
                one_iteration('start ' + self.sqlscript)
                iterations += 1

            # 循环从控制台读取命令
            while True:
                one_iteration()
                iterations += 1
        except EOFError:
            special.close_tee()
            self.echo("Disconnected.")

    def log_output(self, output):
        """Log the output in the audit log, if it's enabled."""
        if self.logfile:
            click.echo(output, file=self.logfile)

    def echo(self, s, **kwargs):
        """Print a message to stdout.

        All keyword arguments are passed to click.echo().

        """
        self.log_output(s)
        click.secho(s, **kwargs)

    def get_output_margin(self, status=None):
        """Get the output margin (number of rows for the prompt, footer and
        timing message."""
        margin = 2

        if status:
            margin += 1 + status.count("\n")

        return margin

    def output(self, output, status=None):
        """Output text to stdout or a pager command.

        The status text is not outputted to pager or files.

        The message will be logged in the audit log, if enabled. The
        message will be written to the tee file, if enabled. The
        message will be written to the output file, if enabled.

        """
        if output:
            size = self.prompt_app.output.get_size()

            margin = self.get_output_margin(status)

            fits = True
            buf = []
            output_via_pager = self.explicit_pager and special.is_pager_enabled()
            for i, line in enumerate(output, 1):
                self.log_output(line)
                special.write_tee(line)
                special.write_once(line)

                if fits or output_via_pager:
                    # buffering
                    buf.append(line)
                    if len(line) > size.columns or i > (size.rows - margin):
                        fits = False
                        if not output_via_pager:
                            # doesn't fit, flush buffer
                            for line in buf:
                                click.secho(line)
                            buf = []
                else:
                    click.secho(line)

            if buf:
                if output_via_pager:
                    # sadly click.echo_via_pager doesn't accept generators
                    click.echo_via_pager("\n".join(buf))
                else:
                    for line in buf:
                        click.secho(line)

        if status:
            self.log_output(status)
            click.secho(status)

    def configure_pager(self):
        # Provide sane defaults for less if they are empty.
        if not os.environ.get("LESS"):
            os.environ["LESS"] = "-RXF"

        cnf = self.read_my_cnf_files(["pager", "skip-pager"])
        if cnf["pager"]:
            special.set_pager(cnf["pager"])
            self.explicit_pager = True
        else:
            self.explicit_pager = False

        if cnf["skip-pager"] or not self.config["main"].as_bool("enable_pager"):
            special.disable_pager()

    def run_query(self, query, new_line=True):
        """Runs *query*."""
        results = self.sqlexecute.run(query)
        for result in results:
            title, cur, headers, status = result
            self.formatter.query = query
            output = self.format_output(title, cur, headers)
            for line in output:
                click.echo(line, nl=new_line)

    def format_output(self, title, cur, headers, expanded=False, max_width=None):
        expanded = expanded or self.formatter.format_name == "vertical"
        output = []

        output_kwargs = {
            "dialect": "unix",
            "disable_numparse": True,
            "preserve_whitespace": True,
            "preprocessors": (preprocessors.align_decimals,),
            "style": self.output_style,
        }

        if title:  # Only print the title if it's not None.
            output = itertools.chain(output, [title])

        if cur:
            column_types = None
            if hasattr(cur, "description"):
                def get_col_type(col):
                    # col_type = FIELD_TYPES.get(col[1], text_type)
                    # return col_type if type(col_type) is type else text_type
                    return str

                column_types = [get_col_type(col) for col in cur.description]

            if max_width is not None:
                cur = list(cur)

            formatted = self.formatter.format_output(
                cur,
                headers,
                format_name="vertical" if expanded else None,
                column_types=column_types,
                **output_kwargs
            )

            if isinstance(formatted, str):
                formatted = formatted.splitlines()
            formatted = iter(formatted)

            first_line = next(formatted)
            formatted = itertools.chain([first_line], formatted)

            if (
                    not expanded
                    and max_width
                    and headers
                    and cur
                    and len(first_line) > max_width
            ):
                formatted = self.formatter.format_output(
                    cur,
                    headers,
                    format_name="vertical",
                    column_types=column_types,
                    **output_kwargs
                )
                if isinstance(formatted, str):
                    formatted = iter(formatted.splitlines())

            output = itertools.chain(output, formatted)

        return output


@click.command()
@click.option("-V", "--version", is_flag=True, help="Output sqlcli's version.")
@click.option(
    "-l",
    "--logfile",
    type=click.File(mode="a", encoding="utf-8"),
    help="Log every query and its results to a file.",
)
@click.option(
    "--sqlclirc",
    default=config_location() + "config",
    help="Location of sqlclirc file.",
    type=click.Path(dir_okay=False),
)
@click.option(
    "--auto-vertical-output",
    is_flag=True,
    help="Automatically switch to vertical output mode if the result is wider than the terminal width.",
)
@click.option(
    "-t", "--table", is_flag=True, help="Display batch output in table format."
)
@click.option("--csv", is_flag=True, help="Display batch output in CSV format.")
@click.option("-e", "--execute", type=str, help="Execute SQL script.")
@click.option("--nologo", is_flag=True, help="Execute with silent mode.")
def cli(
        version,
        logfile,
        auto_vertical_output,
        table,
        csv,
        execute,
        sqlclirc,
        nologo
):
    if version:
        print("Version:", __version__)
        sys.exit(0)

    sqlcli = SQLCli(
        logfile=logfile,
        auto_vertical_output=auto_vertical_output,
        sqlclirc=sqlclirc,
        sqlscript=execute,
        nologo=nologo
    )

    if sys.stdin.isatty():
        sqlcli.run_cli()
    else:
        stdin = click.get_text_stream("stdin")
        stdin_text = stdin.read()

        try:
            sys.stdin = open("/dev/tty")
        except (FileNotFoundError, OSError):
            sqlcli.logger.warning("Unable to open TTY as stdin.")

        try:
            new_line = True

            if csv:
                sqlcli.formatter.format_name = "csv"
            elif not table:
                sqlcli.formatter.format_name = "tsv"

            sqlcli.run_query(stdin_text, new_line=new_line)
            exit(0)
        except Exception as e:
            click.secho(str(e), err=True, fg="red")
            exit(1)


def is_select(status):
    """Returns true if the first word in status is 'select'."""
    if not status:
        return False
    return status.split(None, 1)[0].lower() == "select"


if __name__ == "__main__":
    cli()
