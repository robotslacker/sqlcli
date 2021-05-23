# -*- coding: utf-8 -*-
import os
import sys
import traceback
import click
from .__init__ import __version__
from .sqlcliremoteserver import SQLCliRemoteServer
from .sqlcli import SQLCli
from .sqlcliexception import SQLCliException


@click.command()
@click.option("--version", is_flag=True, help="Output sqlcli's version.")
@click.option("--logon", type=str, help="logon user name and password. user/pass",)
@click.option("--logfile", type=str, help="Log every query and its results to a file.",)
@click.option("--execute", type=str, help="Execute SQL script.")
@click.option("--sqlmap", type=str, help="SQL Mapping file.")
@click.option("--nologo", is_flag=True, help="Execute with nologo mode.")
@click.option("--sqlperf", type=str, help="SQL performance Log.")
@click.option("--syncdriver", is_flag=True, help="Download jdbc jar from file server.")
@click.option("--clientcharset", type=str, help="Set client charset. Default is UTF-8.")
@click.option("--resultcharset", type=str, help="Set result charset. Default is same to clientcharset.")
@click.option("--profile", type=str, help="Init profile.")
@click.option("--scripttimeout", type=int, help="Script Timeout(Seconds)")
@click.option("--server", type=int, help="Set server port, will run it with server mode.")
def cli(
        version,
        logon,
        logfile,
        execute,
        sqlmap,
        nologo,
        sqlperf,
        syncdriver,
        clientcharset,
        resultcharset,
        profile,
        scripttimeout,
        server
):
    if version:
        print("Version:", __version__)
        sys.exit(0)

    if server:
        # 在Server模式下启动应用程序
        SQLCliRemoteServer().Start_SQLCliServer(server)
        sys.exit(0)

    # 从服务器下下载程序需要的各种jar包
    if syncdriver:
        sqlcli = SQLCli(
            logfilename=logfile,
            logon=logon,
            sqlscript=execute,
            sqlmap=sqlmap,
            nologo=nologo,
            sqlperf=sqlperf
        )
        sqlcli.syncdriver()
        sys.exit(0)

    # 程序脚本超时时间设置
    m_ScriptTimeout = -1
    if scripttimeout:
        m_ScriptTimeout = scripttimeout

    sqlcli = SQLCli(
        logfilename=logfile,
        logon=logon,
        sqlscript=execute,
        sqlmap=sqlmap,
        nologo=nologo,
        sqlperf=sqlperf,
        clientcharset=clientcharset,
        resultcharset=resultcharset,
        profile=profile,
        scripttimeout=m_ScriptTimeout
    )

    # 运行主程序
    sqlcli.run_cli()


if __name__ == "__main__":
    try:
        cli()
        sys.exit(0)
    except SQLCliException as se:
        click.secho(se.message, err=True, fg="red")
        sys.exit(1)
    except Exception as ge:
        if "SQLCLI_DEBUG" in os.environ:
            print('traceback.print_exc():\n%s' % traceback.print_exc())
            print('traceback.format_exc():\n%s' % traceback.format_exc())
        click.secho(repr(ge), err=True, fg="red")
        sys.exit(1)
