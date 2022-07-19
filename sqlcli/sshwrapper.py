# -*- coding: utf-8 -*-
import os
import re
import paramiko
from .sqlcliexception import SQLCliException


class SshWrapper(object):
    def __init__(self):
        self.host = None
        self.port = None
        self.username = None
        self.pwd = None
        self.sftp = None
        self.ssh = None
        self.__transport__ = None
        self.shell = None
        self.shell_stdin = None
        self.shell_stdout = None
        self.invokeShell = False

    def sshConnectWithPassword(self, pHostName, pUserName, pPassWord):
        self.host = pHostName
        self.port = 22
        self.username = pUserName
        self.pwd = pPassWord
        func = getattr(paramiko, 'Transport')
        transport = func((self.host, self.port))
        transport.connect(username=self.username, password=self.pwd)
        self.__transport__ = transport
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh._transport = self.__transport__
        self.sftp = paramiko.SFTPClient.from_transport(self.__transport__)

    def sshConnectWithKeyFile(self, pHostName, pUserName, pKeyFile):
        print("UserName=" + pUserName + " keyfile=" + pKeyFile)
        pass

    def sshSetInvokeShell(self, invokeShell):
        if not self.invokeShell and invokeShell:
            self.invokeShell = True
            self.shell = self.ssh.invoke_shell()
            self.shell_stdin = self.shell.makefile('wb')
            self.shell_stdout = self.shell.makefile('r')
            self.shell_stdout.channel.set_combine_stderr(True)
            self.sshExecuteShellCommand("export PS1=$")

    def sshExecuteShellCommand(self, pCommand):
        cmd = pCommand.strip('\n')
        self.shell_stdin.write(cmd + '\n')
        finish = 'end of stdOUT buffer. finished with exit status'
        echo_cmd = 'echo {} $?'.format(finish)
        self.shell_stdin.write(echo_cmd + '\n')
        shin = self.shell_stdin
        self.shell_stdin.flush()

        shout = []
        sherr = []
        for line in self.shell_stdout:
            if str(line).startswith(cmd) or str(line).startswith(echo_cmd):
                # up for now filled with shell junk from stdin
                shout = []
            elif str(line).startswith(finish):
                # our finish command ends with the exit status
                exit_status = int(str(line).rsplit(maxsplit=1)[1])
                if exit_status:
                    # stderr is combined with stdout.
                    # thus, swap sherr with shout in a case of failure.
                    sherr = shout
                    shout = []
                break
            else:
                # get rid of 'coloring and formatting' special characters
                shout.append(re.compile(r'(\x9B|\x1B\[)[0-?]*[ -/]*[@-~]').sub('', line).
                             replace('\b', '').replace('\r', ''))

        # first and last lines of shout/sherr contain a prompt
        if shout and echo_cmd in shout[-1]:
            shout.pop()
        if shout and cmd in shout[0]:
            shout.pop(0)
        if sherr and echo_cmd in sherr[-1]:
            sherr.pop()
        if sherr and cmd in sherr[0]:
            sherr.pop(0)

        return shin, shout, sherr

    def sshExecuteCommand(self, pCommand):
        # 执行命令
        if self.ssh is not None:
            if self.invokeShell:
                _, stdout, _ = self.sshExecuteShellCommand(pCommand)
                for line in stdout:
                    yield str(line.strip('\n'))
            else:
                stdin, stdout, stderr = self.ssh.exec_command(pCommand)
                stdout.channel.set_combine_stderr(True)
                stdin.close()
                consoleOutput = stdout.read().decode('UTF-8')
                for line in consoleOutput.splitlines():
                    yield line
        else:
            yield "SSH not connected."

    def processCommand(self, pSql):
        sql = pSql.strip()

        match_obj = re.match(
            r"ssh\s+set\s+invokeshell\s+(.*?)$",
            sql, re.IGNORECASE | re.DOTALL)
        if match_obj:
            invoke_shell = match_obj.group(1).strip()
            if invoke_shell.upper().strip() == "TRUE":
                self.sshSetInvokeShell(True)
            else:
                self.sshSetInvokeShell(False)

        match_obj = re.match(
            r"ssh\s+connect\s+(.*?)\s+with\s+user\s+(.*?)\s+key\s+(.*?)$",
            sql, re.IGNORECASE | re.DOTALL)
        if match_obj:
            hostname = match_obj.group(1).strip()
            username = match_obj.group(2).strip()
            keyfile = match_obj.group(3).strip()
            self.sshConnectWithPassword(hostname, username, keyfile)
            yield None, None, None, None, "ssh connected."
            return

        match_obj = re.match(
            r"ssh\s+connect\s+(.*?)\s+with\s+user\s+(.*?)\s+password\s+(.*?)$",
            sql, re.IGNORECASE | re.DOTALL)
        if match_obj:
            hostname = match_obj.group(1).strip()
            username = match_obj.group(2).strip()
            password = match_obj.group(3).strip()
            self.sshConnectWithPassword(hostname, username, password)
            yield None, None, None, None, "ssh connected."
            return

        match_obj = re.match(r"ssh\s+execute\s+(.*?)$", sql, re.IGNORECASE | re.DOTALL)
        if match_obj:
            command = match_obj.group(1).strip()
            for consoleOutput in self.sshExecuteCommand(command):
                yield None, None, None, None, consoleOutput
            return

        yield None, None, None, None, "Unknown ssh Command."
