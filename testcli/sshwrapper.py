# -*- coding: utf-8 -*-
import re
import paramiko


class SshWrapper(object):
    def __init__(self):
        self.host = None
        self.port = None
        self.username = None
        self.pwd = None
        self.sftp = None
        self.ssh = None
        self.__transport__ = None

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

    def sshExecuteCommand(self, pCommand):
        # 执行命令
        if self.ssh is not None:
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
