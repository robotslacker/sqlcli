# -*- coding: utf-8 -*-
import os
import sys
import logging
import traceback
from io import open
import jaydebeapi
import re

from cli_helpers.tabular_output import TabularOutputFormatter
from cli_helpers.tabular_output import preprocessors
import click

from prompt_toolkit.shortcuts import PromptSession
from .sqlexecute import SQLExecute
from .sqlinternal import Create_file
from .sqlinternal import Create_SeedCacheFile
from .sqlcliexception import SQLCliException
from .commandanalyze import register_special_command
from .commandanalyze import CommandNotFound

from .__init__ import __version__
from .sqlparse import SQLAnalyze

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
    logon = None
    logfile = None

    def __init__(
            self,
            logon=None,
            logfilename=None,
            sqlscript=None,
            nologo=None
    ):
        self.sqlexecute = SQLExecute()
        if logfilename is not None:
            self.logfile = open(logfilename, mode="a", encoding="utf-8")
            self.sqlexecute.logfile = self.logfile
        self.sqlexecute.sqlscript = sqlscript
        self.sqlscript = sqlscript
        self.nologo = nologo
        self.logon = logon

        self.formatter = TabularOutputFormatter(format_name='ascii')
        self.formatter.sqlcli = self
        self.syntax_style = 'default'
        self.output_style = None

        # 打开日志
        self.logger = logging.getLogger(__name__)

        # 处理一些特殊的命令
        self.register_special_commands()

        self.prompt_app = None

    def register_special_commands(self):

        # 加载数据库驱动
        register_special_command(
            self.load_driver,
            ".loaddriver",
            ".loaddriver",
            "load JDBC driver .",
            aliases=("loaddriver", "\\l"),
        )

        # 连接数据库
        register_special_command(
            self.connect_db,
            ".connect",
            ".connect",
            "Connect to database .",
            aliases=("connect", "\\c"),
        )

        # 从文件中执行脚本
        register_special_command(
            self.execute_from_file,
            "start",
            "\\. filename",
            "Execute commands from file.",
            aliases=("\\.",),
        )

        # 设置各种参数选项
        register_special_command(
            self.set_options,
            "set",
            "set parameter parameter_value",
            "set options .",
            aliases=("set", "\\c"),
        )

        # 执行特殊的命令
        register_special_command(
            self.execute_internal_command,
            "__internal__",
            "execute internal command.",
            "execute internal command.",
            aliases=("__internal__",),
        )

    # 加载JDBC驱动文件
    def load_driver(self, arg, **_):
        if arg is None:
            raise SQLCliException("Missing required argument, load [driver file name] [driver class name].")
        elif arg == "":
            raise SQLCliException("Missing required argument, load [driver file name] [driver class name].")
        else:
            load_parameters = str(arg).split()
            if len(load_parameters) != 2:
                self.logger.debug('arg = ' + str(load_parameters))
                raise SQLCliException("Missing required argument, loaddriver [driver file name] [driver class name].")
            if not os.path.exists(str(load_parameters[0])):
                raise SQLCliException("driver file [" + str(arg) + "] does not exist.")

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
            raise SQLCliException(
                "Missing required argument\n." + "connect [user name]/[password]@" +
                "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
        elif arg == "":
            raise SQLCliException(
                "Missing required argument\n." + "connect [user name]/[password]@" +
                "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
        elif self.jar_file is None:
            raise SQLCliException("Please load driver first.")

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
            self.db_url = \
                connect_parameters[2] + ':' + connect_parameters[3] + ':' + \
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
                        self.db_url = \
                            connect_parameters[0] + ':' + connect_parameters[1] + ':' +\
                            connect_parameters[2] + '://' + connect_parameters[3] + ':' + \
                            connect_parameters[4] + ':/' + connect_parameters[5]
                    else:
                        print("db_type = [" + str(self.db_type) + "]")
                        print("db_host = [" + str(self.db_host) + "]")
                        print("db_port = [" + str(self.db_port) + "]")
                        print("db_service_name = [" + str(self.db_service_name) + "]")
                        print("db_url = [" + str(self.db_url) + "]")
                        raise SQLCliException("Unexpeced env SQLCLI_CONNECTION_URL\n." +
                                              "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
                else:
                    # 用户第一次连接，而且没有指定环境变量
                    raise SQLCliException("Missing required argument\n." + "connect [user name]/[password]@" +
                                          "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")

        # 连接数据库
        try:
            if self.db_type.upper() == "ORACLE":
                self.db_conn = jaydebeapi.connect(self.driver_class,
                                                  'jdbc:' + self.db_type + ":" + self.db_driver_type + ":@" +
                                                  self.db_host + ":" + self.db_port + "/" + self.db_service_name,
                                                  [self.db_username, self.db_password],
                                                  self.jar_file, )
            elif self.db_type.upper() == "LINKOOPDB":
                self.db_conn = jaydebeapi.connect(self.driver_class,
                                                  'jdbc:' + self.db_type + ":" + self.db_driver_type + "://" +
                                                  self.db_host + ":" + self.db_port + "/" + self.db_service_name +
                                                  ";query_iterator=1",
                                                  [self.db_username, self.db_password],
                                                  self.jar_file, )
            else:
                self.db_conn = jaydebeapi.connect(self.driver_class,
                                                  'jdbc:' + self.db_type + ":" + self.db_driver_type + "://" +
                                                  self.db_host + ":" + self.db_port + "/" + self.db_service_name,
                                                  [self.db_username, self.db_password],
                                                  self.jar_file, )
            self.sqlexecute.set_connection(self.db_conn)
        except Exception as e:  # Connecting to a database fail.
            print('traceback.print_exc():\n%s' % traceback.print_exc())
            print('traceback.format_exc():\n%s' % traceback.format_exc())
            print("db_type = [" + str(self.db_type) + "]")
            print("db_host = [" + str(self.db_host) + "]")
            print("db_port = [" + str(self.db_port) + "]")
            print("db_service_name = [" + str(self.db_service_name) + "]")
            print("db_url = [" + str(self.db_url) + "]")
            raise SQLCliException(repr(e))

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
        if arg is None:
            raise Exception("Missing required argument. set parameter parameter_value.")
        elif arg == "":
            m_Result = []
            for key, value in self.sqlexecute.options.items():
                m_Result.append([str(key), str(value)])
            yield (
                "Current set options: ",
                m_Result,
                ["option", "value"],
                ""
            )
        else:
            options_parameters = str(arg).split()
            if len(options_parameters) != 2:
                raise Exception("Missing required argument. set parameter parameter_value.")
            # 如果不是已知的选项，则直接抛出到SQL引擎
            if options_parameters[0].upper() in self.sqlexecute.options:
                self.sqlexecute.options[options_parameters[0].upper()] = options_parameters[1].upper()
                yield (
                    None,
                    None,
                    None,
                    '')
            else:
                raise CommandNotFound

    # 执行特殊的命令
    def execute_internal_command(self, arg, **_):
        # 去掉回车换行符，以及末尾的空格
        strSQL = str(arg).replace('\r', '').replace('\n', '').strip()

        # 创建数据文件
        matchObj = re.match(r"create\s+file\s+(.*?)\((.*)\)(\s+)?rows\s+([1-9]\d*)(\s+)?(;)?$",
                            strSQL, re.IGNORECASE)
        if matchObj:
            # create file command  将根据格式要求创建需要的文件
            Create_file(p_filename=str(matchObj.group(1)),
                        p_formula_str=str(matchObj.group(2)),
                        p_rows=int(matchObj.group(4)),
                        p_options=self.sqlexecute.options)
            yield (
                None,
                None,
                None,
                str(matchObj.group(4)) + ' rows created Successful.')
            return

        # 创建随机数Seed的缓存文件
        matchObj = re.match(r"create\s+seeddatafile(\s+)?;$",
                            strSQL, re.IGNORECASE)
        if matchObj:
            Create_SeedCacheFile()
            yield (
                None,
                None,
                None,
                'file created Successful.')
            return

        # 不认识的internal命令
        raise SQLCliException("Unknown internal Command. Please double check.")

    # 主程序
    def run_cli(self):
        iterations = 0

        # 给Page做准备，PAGE显示的默认换页方式.
        if not os.environ.get("LESS"):
            os.environ["LESS"] = "-RXF"

        if not self.nologo:
            print("SQL*Cli Release " + __version__)

        def one_iteration(text=None):
            # 判断传入SQL语句
            if text is None:
                full_text = None
                while True:
                    # 用户一行一行的输入SQL语句
                    try:
                        if full_text is None:
                            text = self.prompt_app.prompt('SQL> ')
                        else:
                            text = self.prompt_app.prompt('   > ')
                    except KeyboardInterrupt:
                        return
                    # 拼接SQL语句
                    if full_text is None:
                        full_text = text
                    else:
                        full_text = full_text + '\n' + text
                    # 判断SQL语句是否已经结束
                    (ret_bSQLCompleted, ret_SQLSplitResults, ret_SQLSplitResultsWithComments) = SQLAnalyze(full_text)
                    if ret_bSQLCompleted:
                        # SQL 语句已经结束
                        break
                text = full_text

            # 如果文本是空行，直接跳过
            if not text.strip():
                return

            try:
                res = self.sqlexecute.run(text)

                self.formatter.query = text
                result_count = 0
                for title, cur, headers, status in res:
                    # prompt_app 默认列最长的宽度是119
                    # max_width = self.prompt_app.output.get_size().columns
                    max_width = None

                    # title 包含原有语句的SQL信息，如果ECHO打开的话
                    # headers 包含原有语句的列名
                    # cur 是语句的执行结果
                    # output_format 输出格式
                    #   ascii              默认，即表格格式
                    #   vertical           分行显示，每行、每列都分行
                    #   csv                csv格式显示
                    formatted = self.format_output(
                        title, cur, headers,
                        self.sqlexecute.options["OUTPUT_FORMAT"].lower(),
                        max_width
                    )

                    try:
                        if result_count > 0:
                            self.echo("")
                        try:
                            self.output(formatted, status)
                        except KeyboardInterrupt:
                            pass
                    except KeyboardInterrupt:
                        pass

                    result_count += 1
            except EOFError as e:
                raise e
            except NotImplementedError:
                self.echo("Not Yet Implemented.", fg="yellow")
            except Exception as e:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
                self.echo(str(e), err=True, fg="red")

        self.prompt_app = PromptSession()

        try:
            # 如果环境变量中包含了SQLCLI_CONNECTION_JAR_NAME或者SQLCLI_CONNECTION_CLASS_NAME
            # 则直接加载
            if "SQLCLI_CONNECTION_JAR_NAME" in os.environ and \
                    "SQLCLI_CONNECTION_CLASS_NAME" in os.environ:
                one_iteration('loaddriver ' +
                              os.environ["SQLCLI_CONNECTION_JAR_NAME"] + ' ' + os.environ[
                                  "SQLCLI_CONNECTION_CLASS_NAME"])
                iterations += 1

            # 如果用户制定了用户名，口令，尝试直接进行数据库连接
            if self.logon:
                one_iteration("connect " + str(self.logon))

            # 如果传递的参数中有SQL文件，先执行SQL文件, 执行完成后自动退出
            if self.sqlscript:
                one_iteration('start ' + self.sqlscript)
                iterations += 1
                one_iteration('exit')
                iterations += 1

            # 循环从控制台读取命令
            while True:
                one_iteration()
                iterations += 1
        except EOFError:
            self.echo("Disconnected.")

    def log_output(self, output):
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
        """
        Output text to stdout or a pager command.

        The status text is not outputted to pager or files.

        The message will be written to the output file, if enabled.

        """
        if output:
            # size    记录了 每页输出最大行数，以及行的宽度。  Size(rows=30, columns=119)
            # margin  记录了每页需要留下多少边界行，如状态显示信息等 （2 或者 3）
            size = self.prompt_app.output.get_size()
            margin = self.get_output_margin(status)

            # 打印输出信息
            fits = True
            buf = []
            output_via_pager = ((self.sqlexecute.options["PAGE"]).upper() == "ON")
            for i, line in enumerate(output, 1):
                self.log_output(line)       # 输出文件中总是不考虑分页问题
                if fits or output_via_pager:
                    # buffering
                    buf.append(line)
                    if len(line) > size.columns or i > (size.rows - margin):
                        # 如果行超过页要求，或者行内容过长，且没有分页要求的话，直接显示
                        fits = False
                        if not output_via_pager:
                            # doesn't fit, flush buffer
                            for bufline in buf:
                                click.secho(bufline)
                            buf = []
                else:
                    click.secho(line)

            if buf:
                if output_via_pager:
                    click.echo_via_pager("\n".join(buf))
                else:
                    for line in buf:
                        click.secho(line)

        if status:
            self.log_output(status)
            click.secho(status)

    def format_output(self, title, cur, headers, p_format_name, max_width=None):
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
            # 列的数据类型，如果不存在，按照None来处理
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
                format_name=p_format_name,
                column_types=column_types,
                **output_kwargs
            )
            if isinstance(formatted, str):
                formatted = formatted.splitlines()
            formatted = iter(formatted)

            # 获得输出信息的首行
            first_line = next(formatted)
            # 获得输出信息的格式控制
            formatted = itertools.chain([first_line], formatted)
            # 返回输出信息
            output = itertools.chain(output, formatted)
        return output


@click.command()
@click.option("--version", is_flag=True, help="Output sqlcli's version.")
@click.option(
    "--logon",
    type=str,
    help="logon user name and password. user/pass",
)
@click.option(
    "--logfile",
    type=str,
    help="Log every query and its results to a file.",
)
@click.option("--execute", type=str, help="Execute SQL script.")
@click.option("--nologo", is_flag=True, help="Execute with silent mode.")
def cli(
        version,
        logon,
        logfile,
        execute,
        nologo
):
    if version:
        print("Version:", __version__)
        sys.exit(0)

    sqlcli = SQLCli(
        logfilename=logfile,
        logon=logon,
        sqlscript=execute,
        nologo=nologo
    )

    # 运行主程序
    sqlcli.run_cli()


if __name__ == "__main__":
    cli()
