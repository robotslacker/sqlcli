# -*- coding: UTF-8 -*-
import re
import os
import datetime

import fnmatch
import traceback
from glob import glob

from hdfs import InsecureClient, HdfsError
from .sqlcliexception import SQLCliException


class HDFSWrapperException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class HDFSWrapper(object):

    def __init__(self):
        self.__m_HDFS_Handler__ = None
        self.__m_HDFS_WebFSDir__ = None

    def HDFS_makedirs(self, hdfs_path):
        """ 创建目录 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSWrapperException("HDFS not connected. Please connect it frist.")
        self.__m_HDFS_Handler__.makedirs(os.path.join(self.__m_HDFS_WebFSDir__, hdfs_path).replace('\\', '/'))

    def HDFS_setPermission(self, hdfs_path, permission):
        """ 修改指定文件的权限信息 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSWrapperException("HDFS not connected. Please connect it frist.")
        m_hdfs_filepath = os.path.dirname(hdfs_path)
        m_hdfs_filename = os.path.basename(hdfs_path)
        self.__m_HDFS_Handler__.set_permission(
            os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath, m_hdfs_filename).replace('\\', '/'),
            permission=permission)


    def HDFS_Connect(self, p_szURL, p_szUser):
        """ 连接HDFS, URL使用WEBFS协议 """
        m_HDFS_Protocal = p_szURL.split("://")[0]
        m_HDFS_NodePort = p_szURL[len(m_HDFS_Protocal) + 3:].split("/")[0]
        m_HDFS_WebFSURL = m_HDFS_Protocal + "://" + m_HDFS_NodePort
        self.__m_HDFS_WebFSDir__ = p_szURL[len(m_HDFS_WebFSURL):]
        self.__m_HDFS_Handler__ = InsecureClient(url=m_HDFS_WebFSURL,
                                                 user=p_szUser,
                                                 root=self.__m_HDFS_WebFSDir__)
        # 尝试创建目录，如果目录不存在的话
        self.__m_HDFS_Handler__.makedirs(self.__m_HDFS_WebFSDir__.replace('\\', '/'))

    def HDFS_status(self, hdfs_path=""):
        """ 返回目录下的文件 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSWrapperException("HDFS not connected. Please connect it frist.")

        m_ReturnList = []
        m_Status = self.__m_HDFS_Handler__.status(hdfs_path)
        m_ReturnList.append((hdfs_path, m_Status))
        return m_ReturnList

    def HDFS_list(self, hdfs_path="", recusive=False):
        """ 返回目录下的文件 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSWrapperException("HDFS not connected. Please connect it frist.")

        m_ReturnList = []
        if not recusive:
            for row in self.__m_HDFS_Handler__.list(hdfs_path, status=True):
                m_ReturnList.append((os.path.join(hdfs_path, row[0]), row[1]))
            return m_ReturnList
        else:
            for row in self.__m_HDFS_Handler__.list(hdfs_path, status=True):
                if row[1]['type'].upper() == 'DIRECTORY':
                    m_ReturnList.append((os.path.join(hdfs_path, row[0]).replace("\\", "/"), row[1]))
                    m_ReturnList.extend(
                        self.HDFS_list(os.path.join(hdfs_path, row[0]).replace("\\", "/"),
                                       recusive=True)
                    )
                else:
                    m_ReturnList.append((os.path.join(hdfs_path, row[0]).replace("\\", "/"), row[1]))
            return m_ReturnList

    def HDFS_Download(self, hdfs_path="", local_path="", recusive=False):
        """ 从hdfs获取文件到本地 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSWrapperException("HDFS not connected. Please connect it frist.")

        # 如果本地没有对应目录，且local_path传递的是一个目录，则建立目录
        m_LocalPath = local_path
        if m_LocalPath.endswith("/") and not os.path.exists(m_LocalPath):
            os.makedirs(m_LocalPath)

        m_FileList = self.HDFS_list(recusive=recusive)
        for row in m_FileList:
            if fnmatch.fnmatch(row[0], hdfs_path):
                self.__m_HDFS_Handler__.download(row[0], m_LocalPath, overwrite=True)

    def HDFS_Upload(self, local_path, hdfs_path=""):
        """ 上传文件到hdfs """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSWrapperException("HDFS not connected. Please connect it frist.")

        for file in glob(local_path):
            if hdfs_path == "":
                m_hdfs_filepath = ""
                m_hdfs_filename = os.path.basename(file)
            else:
                if hdfs_path.endswith("/"):
                    m_hdfs_filepath = hdfs_path
                    m_hdfs_filename = os.path.basename(file)
                else:
                    m_hdfs_filepath = os.path.dirname(hdfs_path)
                    m_hdfs_filename = os.path.basename(hdfs_path)
            try:
                remote_status = self.__m_HDFS_Handler__.status(
                    os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath, m_hdfs_filename).replace('\\', '/'))
                if remote_status['type'] == "DIRECTORY":
                    # 远程目录已经存在， 会尝试删除这个目录
                    self.__m_HDFS_Handler__.delete(
                        os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath, m_hdfs_filename).replace('\\', '/'),
                        recursive=True)
            except HdfsError:
                # 远程目录不存在，后续的upload会建立该目录
                pass
            self.__m_HDFS_Handler__.upload(
                os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath, m_hdfs_filename).replace('\\', '/'),
                file,
                overwrite=True,
                cleanup=True)

    def Process_SQLCommand(self, p_szSQL):
        try:
            m_szSQL = p_szSQL.strip()
            matchObj = re.match(r"hdfs\s+connect\s+(.*)\s+with\s+user\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_HDFSServer = str(matchObj.group(1)).strip()
                m_HDFSUser = str(matchObj.group(2)).strip()
                self.HDFS_Connect(m_HDFSServer, m_HDFSUser)
                return None, None, None, None, "Hdfs Server set successful."

            matchObj = re.match(r"hdfs\s+status\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_TargetFileList = str(matchObj.group(1)).strip()
                m_ReturnFileList = self.HDFS_status(m_TargetFileList)
                m_Result = []
                for (m_FileName, m_FileProperties) in m_ReturnFileList:
                    m_PermissionMask = ""
                    if m_FileProperties["type"] == "FILE":
                        m_PermissionMask = "-"
                    elif m_FileProperties["type"] == "DIRECTORY":
                        m_PermissionMask = "d"
                    else:
                        m_PermissionMask = "?"
                    if len(m_FileProperties["permission"]) == 3:
                        for m_nPos in range(0, 3):
                            if m_FileProperties["permission"][m_nPos] == "0":
                                m_PermissionMask = m_PermissionMask + "---"
                            elif m_FileProperties["permission"][m_nPos] == "1":
                                m_PermissionMask = m_PermissionMask + "--x"
                            elif m_FileProperties["permission"][m_nPos] == "2":
                                m_PermissionMask = m_PermissionMask + "-w-"
                            elif m_FileProperties["permission"][m_nPos] == "3":
                                m_PermissionMask = m_PermissionMask + "-wx"
                            elif m_FileProperties["permission"][m_nPos] == "4":
                                m_PermissionMask = m_PermissionMask + "r--"
                            elif m_FileProperties["permission"][m_nPos] == "5":
                                m_PermissionMask = m_PermissionMask + "r-x"
                            elif m_FileProperties["permission"][m_nPos] == "6":
                                m_PermissionMask = m_PermissionMask + "rw-"
                            elif m_FileProperties["permission"][m_nPos] == "7":
                                m_PermissionMask = m_PermissionMask + "rwx"
                            else:
                                m_PermissionMask = m_PermissionMask + "???"
                    else:
                        m_PermissionMask = m_PermissionMask + "?????????"
                    m_ModifiedTime = str(datetime.datetime.utcfromtimestamp(
                        m_FileProperties["modificationTime"] / 1000).strftime("%Y-%m-%d %H:%M:%S"))
                    m_Result.append([m_TargetFileList,
                                     m_PermissionMask,
                                     m_FileProperties["owner"], m_FileProperties["group"],
                                     m_FileProperties["length"], m_ModifiedTime])
                return "HDFS file status:", m_Result, ["Path", "Permission", "owner", "group", "Size", "Modified"], \
                       None, "Total " + str(len(m_Result)) + " files listed."

            matchObj = re.match(r"hdfs\s+rm\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                if matchObj:
                    m_FileDeleted = str(matchObj.group(1)).strip()
                    m_FileList = self.HDFS_list(recusive=False)
                    for row in m_FileList:
                        if fnmatch.fnmatch(row[0], m_FileDeleted):
                            self.__m_HDFS_Handler__.delete(row[0], recursive=True)
                return None, None, None, None, "Hdfs file deleted successful."

            matchObj = re.match(r"hdfs\s+makedirs\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_Dir = str(matchObj.group(1)).strip()
                self.HDFS_makedirs(m_Dir)
                return None, None, None, None, "Hdfs directory created successful."

            matchObj = re.match(r"hdfs\s+set_permission\s+(.*)\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_File = str(matchObj.group(1)).strip()
                m_FilePermission = str(matchObj.group(2)).strip()
                self.HDFS_setPermission(m_File, m_FilePermission)
                return None, None, None, None, "Hdfs set permission successful."

            m_FileUpload = ""
            m_TargetDir = None
            matchObj = re.match(r"hdfs\s+upload\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_FileUpload = str(matchObj.group(1)).strip()
                m_TargetDir = ""
            matchObj = re.match(r"hdfs\s+upload\s+(.*)\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_FileUpload = str(matchObj.group(1)).strip()
                m_TargetDir = str(matchObj.group(2)).strip()
            if m_TargetDir is not None:
                self.HDFS_Upload(m_FileUpload, m_TargetDir)
                return None, None, None, None, "Hdfs file upload successful."

            m_FileDownload = ""
            m_TargetDir = None
            matchObj = re.match(r"hdfs\s+download\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_FileDownload = str(matchObj.group(1)).strip()
                m_TargetDir = ""
            matchObj = re.match(r"hdfs\s+download\s+(.*)\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_FileDownload = str(matchObj.group(1)).strip()
                m_TargetDir = str(matchObj.group(2)).strip()
            if m_TargetDir is not None:
                self.HDFS_Download(m_FileDownload, m_TargetDir)
                return None, None, None, None, "Hdfs file download successful."

            m_TargetFileList = None
            matchObj = re.match(r"hdfs\s+list(\s+)?$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_TargetFileList = ""
            matchObj = re.match(r"hdfs\s+list\s+(.*)?$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_TargetFileList = str(matchObj.group(1)).strip()
            if m_TargetFileList is not None:
                m_ReturnFileList = self.HDFS_list(m_TargetFileList, recusive=True)
                m_Result = []
                for (m_FileName, m_FileProperties) in m_ReturnFileList:
                    m_PermissionMask = ""
                    if m_FileProperties["type"] == "FILE":
                        m_PermissionMask = "-"
                    elif m_FileProperties["type"] == "DIRECTORY":
                        m_PermissionMask = "d"
                    else:
                        m_PermissionMask = "?"
                    if len(m_FileProperties["permission"]) == 3:
                        for m_nPos in range(0, 3):
                            if m_FileProperties["permission"][m_nPos] == "0":
                                m_PermissionMask = m_PermissionMask + "---"
                            elif m_FileProperties["permission"][m_nPos] == "1":
                                m_PermissionMask = m_PermissionMask + "--x"
                            elif m_FileProperties["permission"][m_nPos] == "2":
                                m_PermissionMask = m_PermissionMask + "-w-"
                            elif m_FileProperties["permission"][m_nPos] == "3":
                                m_PermissionMask = m_PermissionMask + "-wx"
                            elif m_FileProperties["permission"][m_nPos] == "4":
                                m_PermissionMask = m_PermissionMask + "r--"
                            elif m_FileProperties["permission"][m_nPos] == "5":
                                m_PermissionMask = m_PermissionMask + "r-x"
                            elif m_FileProperties["permission"][m_nPos] == "6":
                                m_PermissionMask = m_PermissionMask + "rw-"
                            elif m_FileProperties["permission"][m_nPos] == "7":
                                m_PermissionMask = m_PermissionMask + "rwx"
                            else:
                                m_PermissionMask = m_PermissionMask + "???"
                    else:
                        m_PermissionMask = m_PermissionMask + "?????????"
                    m_ModifiedTime = str(datetime.datetime.utcfromtimestamp(
                        m_FileProperties["modificationTime"]/1000).strftime("%Y-%m-%d %H:%M:%S"))
                    m_Result.append([m_FileProperties["pathSuffix"],
                                     m_PermissionMask,
                                     m_FileProperties["owner"], m_FileProperties["group"],
                                     m_FileProperties["length"], m_ModifiedTime])
                return "HDFS file List:", m_Result, ["Path", "Permission", "owner", "group", "Size", "Modified"], \
                       None, "Total " + str(len(m_Result)) + " files listed."
            return None, None, None, None, "Unknown HDFS Command."
        except (HDFSWrapperException, HdfsError) as he:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
            raise SQLCliException(he.message)