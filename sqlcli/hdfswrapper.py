# -*- coding: UTF-8 -*-
import re
import os
import datetime

import fnmatch
import traceback
from glob import glob

from hdfs import InsecureClient, HdfsError


class HDFSWrapperException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class HDFSWrapper(object):

    def __init__(self):
        self.__m_HDFS_Handler__ = None
        self.__m_HDFS_WebFSDir__ = None
        self.__m_HDFS_User__ = None
        self.__m_HDFS_WebFSURL__ = None
        self.__mLocalRootDirectory = None                       # 本地文件的初始目录

    def HDFS_makedirs(self, hdfs_path):
        """ 创建目录 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSWrapperException("HDFS not connected. Please connect it first.")
        self.__m_HDFS_Handler__.makedirs(os.path.join(self.__m_HDFS_WebFSDir__, hdfs_path).replace('\\', '/'))

    def HDFS_setPermission(self, hdfs_path, permission):
        """ 修改指定文件的权限信息 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSWrapperException("HDFS not connected. Please connect it first.")
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
        self.__m_HDFS_User__ = p_szUser
        self.__m_HDFS_WebFSURL__ = m_HDFS_WebFSURL
        self.__m_HDFS_WebFSDir__ = p_szURL[len(m_HDFS_WebFSURL):]
        self.__m_HDFS_Handler__ = InsecureClient(url=m_HDFS_WebFSURL,
                                                 user=p_szUser,
                                                 root=self.__m_HDFS_WebFSDir__)
        # 尝试创建目录，如果目录不存在的话
        self.__m_HDFS_Handler__.makedirs(self.__m_HDFS_WebFSDir__.replace('\\', '/'))

    # 切换远程HDFS的文件目录
    def HDFS_CD(self, p_szPath):
        self.__m_HDFS_WebFSDir__ = os.path.join(self.__m_HDFS_WebFSDir__, p_szPath)
        self.__m_HDFS_Handler__ = InsecureClient(url=self.__m_HDFS_WebFSURL__,
                                                 user=self.__m_HDFS_User__,
                                                 root=self.__m_HDFS_WebFSDir__)
        # 尝试创建目录，如果目录不存在的话
        self.__m_HDFS_Handler__.makedirs(self.__m_HDFS_WebFSDir__.replace('\\', '/'))

    # 切换本地的文件目录
    def HDFS_LCD(self, p_szPath):
        self.__mLocalRootDirectory = p_szPath

    def HDFS_status(self, hdfs_path=""):
        """ 返回目录下的文件 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSWrapperException("HDFS not connected. Please connect it first.")

        m_ReturnList = []
        m_Status = self.__m_HDFS_Handler__.status(hdfs_path=hdfs_path, strict=False)
        if m_Status is None:
            raise HDFSWrapperException("File or directory [" + str(hdfs_path) + "] does not exist!.")
        if m_Status['type'].upper() == 'DIRECTORY':
            # HDFS CLI对于目录的大小总是返回0， 所以这里遍历所有的目录来获得准确的目录大小
            m_FileList = self.HDFS_list(hdfs_path, recusive=True)
            m_FileSize = 0
            for m_File in m_FileList:
                m_FileSize = m_FileSize + int(m_File[1]['length'])
            if m_FileSize > 1024*1024*1024:
                m_FileSize = str(round(float(m_FileSize) / (1024*1024*1024))) + 'G'
            elif m_FileSize > 1024*1024:
                m_FileSize = str(round(float(m_FileSize) / (1024*1024))) + 'M'
            elif m_FileSize > 1024:
                m_FileSize = str(round(float(m_FileSize) / 1024)) + 'K'
            else:
                m_FileSize = str(m_FileSize)
            m_Status['length'] = m_FileSize
        m_ReturnList.append((hdfs_path, m_Status))
        return m_ReturnList

    def HDFS_list(self, hdfs_path="", recusive=False):
        """ 返回目录下的文件 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSWrapperException("HDFS not connected. Please connect it first.")

        # 先用Content函数判断文件或者目录是否存在
        m_Content = self.__m_HDFS_Handler__.content(hdfs_path=hdfs_path, strict=False)
        if m_Content is None:
            raise HDFSWrapperException("File or directory [" + str(hdfs_path) + "] does not exist!.")

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
            raise HDFSWrapperException("HDFS not connected. Please connect it first.")

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
            raise HDFSWrapperException("HDFS not connected. Please connect it first.")

        if self.__mLocalRootDirectory is not None:
            mLocalPath = os.path.join(str(self.__mLocalRootDirectory), local_path)
        else:
            mLocalPath = local_path
        if not os.path.exists(mLocalPath):
            raise HDFSWrapperException("HDFS upload failed. "
                                       "Local file or directory [" + str(mLocalPath) + " ] does not exist.")

        if os.path.isdir(mLocalPath):
            if hdfs_path == "" or hdfs_path.endswith("/"):
                m_hdfs_filepath = hdfs_path
            else:
                raise HDFSWrapperException("HDFS upload failed. " +
                                           "You can not upload a directory [" + str(mLocalPath) + " ] " +
                                           "to remote file [" + hdfs_path + "].")
            try:
                remote_status = self.__m_HDFS_Handler__.status(
                    hdfs_path=os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath).replace('\\', '/'),
                    strict=True)
                if remote_status['type'] == "FILE":
                    # 远程以为是目录的地方其实放了一个奇怪的文件，于是删掉它
                    self.__m_HDFS_Handler__.delete(
                        os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath).replace('\\', '/'), recursive=True)
                remote_status = self.__m_HDFS_Handler__.status(
                    os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath).replace('\\', '/'))
                if remote_status['type'] == "DIRECTORY":
                    # 远程目录已经存在， 会尝试删除这个目录
                    self.__m_HDFS_Handler__.delete(
                        os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath).replace('\\', '/'),
                        recursive=True)
            except HdfsError:
                # 远程目录不存在，后续的upload会建立该目录
                pass
            for root, dirs, files in os.walk(mLocalPath):
                mRelPath = os.path.relpath(root, self.__mLocalRootDirectory)
                # 直接上传整个目录，会无法上传空目录（HDFS限制），所以这里遍历循环上传
                if len(files) == 0:
                    # 空目录，远程建立该空目录
                    memptydir = os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath, mRelPath).replace('\\', '/')
                    if "SQLCLI_DEBUG" in os.environ:
                        print("[DEBUG] HDFS create empty dir " + str(memptydir))
                    self.__m_HDFS_Handler__.makedirs(memptydir)
                for f in files:
                    mlocalfile = os.path.join(root, f)
                    mremotefile = os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath, mRelPath, f).\
                        replace('\\', '/')
                    if "SQLCLI_DEBUG" in os.environ:
                        print("[DEBUG] HDFS upload local  " + str(mlocalfile) + " to remote " + str(mremotefile))
                    self.__m_HDFS_Handler__.upload(
                        hdfs_path=mremotefile,
                        local_path=mlocalfile,
                        overwrite=True,
                        cleanup=True)
        else:
            for file in glob(mLocalPath):
                # 指定的是一个或者带有通配符的文件
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
                        hdfs_path=os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath).replace('\\', '/'),
                        strict=True)
                    if remote_status['type'] == "FILE":
                        # 远程以为是目录的地方其实放了一个奇怪的文件，于是删掉它
                        self.__m_HDFS_Handler__.delete(
                            os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath).replace('\\', '/'), recursive=True)
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
            match_obj = re.match(r"hdfs\s+connect\s+(.*)\s+with\s+user\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if match_obj:
                m_HDFSServer = str(match_obj.group(1)).strip()
                m_HDFSUser = str(match_obj.group(2)).strip()
                self.HDFS_Connect(m_HDFSServer, m_HDFSUser)
                return None, None, None, None, "Hdfs Server set successful."

            match_obj = re.match(r"hdfs\s+cd\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if match_obj:
                m_HDFSPath = str(match_obj.group(1)).strip()
                self.HDFS_CD(m_HDFSPath)
                return None, None, None, None, "Hdfs root dir change successful."

            match_obj = re.match(r"hdfs\s+status\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if match_obj:
                m_TargetFileList = str(match_obj.group(1)).strip()
                m_ReturnFileList = self.HDFS_status(m_TargetFileList)
                m_Result = []
                for (m_FileName, m_FileProperties) in m_ReturnFileList:
                    if m_FileProperties["type"] == "FILE":
                        m_PermissionMask = "-"
                    elif m_FileProperties["type"] == "DIRECTORY":
                        m_PermissionMask = "d"
                    else:
                        m_PermissionMask = "?"
                    if len(m_FileProperties["permission"]) == 3:
                        for pos in range(0, 3):
                            if m_FileProperties["permission"][pos] == "0":
                                m_PermissionMask = m_PermissionMask + "---"
                            elif m_FileProperties["permission"][pos] == "1":
                                m_PermissionMask = m_PermissionMask + "--x"
                            elif m_FileProperties["permission"][pos] == "2":
                                m_PermissionMask = m_PermissionMask + "-w-"
                            elif m_FileProperties["permission"][pos] == "3":
                                m_PermissionMask = m_PermissionMask + "-wx"
                            elif m_FileProperties["permission"][pos] == "4":
                                m_PermissionMask = m_PermissionMask + "r--"
                            elif m_FileProperties["permission"][pos] == "5":
                                m_PermissionMask = m_PermissionMask + "r-x"
                            elif m_FileProperties["permission"][pos] == "6":
                                m_PermissionMask = m_PermissionMask + "rw-"
                            elif m_FileProperties["permission"][pos] == "7":
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

            match_obj = re.match(r"hdfs\s+rm\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if match_obj:
                # 不支持在rm的路径中加入通配符，但是最后文件名可以包含通配符
                m_Bak_WebFSDir = self.__m_HDFS_WebFSDir__
                m_FileDeleted = str(match_obj.group(1)).strip()
                m_FileDeletedPath = os.path.dirname(m_FileDeleted)
                m_FileDeletedName = os.path.basename(m_FileDeleted)
                if len(m_FileDeletedName.strip()) == 0:
                    # 计划删除的是一个目录，而不是文件
                    self.__m_HDFS_Handler__.delete(m_FileDeletedPath, recursive=True)
                    return None, None, None, None, "Hdfs file deleted successful."

                self.HDFS_CD(m_FileDeletedPath)
                m_FileList = self.HDFS_list(self.__m_HDFS_WebFSDir__, recusive=True)
                for row in m_FileList:
                    if fnmatch.fnmatch(os.path.basename(row[0]), m_FileDeletedName):
                        self.__m_HDFS_Handler__.delete(m_FileDeletedName, recursive=True)
                # 重新返回原目录
                self.HDFS_CD(m_Bak_WebFSDir)
                return None, None, None, None, "Hdfs file deleted successful."

            match_obj = re.match(r"hdfs\s+makedirs\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if match_obj:
                m_Dir = str(match_obj.group(1)).strip()
                self.HDFS_makedirs(m_Dir)
                return None, None, None, None, "Hdfs directory created successful."

            match_obj = re.match(r"hdfs\s+set_permission\s+(.*)\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if match_obj:
                m_File = str(match_obj.group(1)).strip()
                m_FilePermission = str(match_obj.group(2)).strip()
                self.HDFS_setPermission(m_File, m_FilePermission)
                return None, None, None, None, "Hdfs set permission successful."

            m_FileUpload = ""
            m_TargetDir = None
            match_obj = re.match(r"hdfs\s+upload\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if match_obj:
                m_FileUpload = str(match_obj.group(1)).strip()
                m_TargetDir = ""
            match_obj = re.match(r"hdfs\s+upload\s+(.*)\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if match_obj:
                m_FileUpload = str(match_obj.group(1)).strip()
                m_TargetDir = str(match_obj.group(2)).strip()
            if m_TargetDir is not None:
                self.HDFS_Upload(m_FileUpload, m_TargetDir)
                return None, None, None, None, "Hdfs file upload successful."

            m_FileDownload = ""
            m_TargetDir = None
            match_obj = re.match(r"hdfs\s+download\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if match_obj:
                m_FileDownload = str(match_obj.group(1)).strip()
                m_TargetDir = ""
            match_obj = re.match(r"hdfs\s+download\s+(.*)\s+(.*)$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if match_obj:
                m_FileDownload = str(match_obj.group(1)).strip()
                m_TargetDir = str(match_obj.group(2)).strip()
            if m_TargetDir is not None:
                self.HDFS_Download(m_FileDownload, m_TargetDir)
                return None, None, None, None, "Hdfs file download successful."

            m_TargetFileList = None
            match_obj = re.match(r"hdfs\s+list(\s+)?$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if match_obj:
                m_TargetFileList = ""
            match_obj = re.match(r"hdfs\s+list\s+(.*)?$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if match_obj:
                m_TargetFileList = str(match_obj.group(1)).strip()
            if m_TargetFileList is not None:
                m_ReturnFileList = self.HDFS_list(m_TargetFileList, recusive=True)
                m_Result = []
                for (m_FileName, m_FileProperties) in m_ReturnFileList:
                    if m_FileProperties["type"] == "FILE":
                        m_PermissionMask = "-"
                    elif m_FileProperties["type"] == "DIRECTORY":
                        m_PermissionMask = "d"
                    else:
                        m_PermissionMask = "?"
                    if len(m_FileProperties["permission"]) == 3:
                        for pos in range(0, 3):
                            if m_FileProperties["permission"][pos] == "0":
                                m_PermissionMask = m_PermissionMask + "---"
                            elif m_FileProperties["permission"][pos] == "1":
                                m_PermissionMask = m_PermissionMask + "--x"
                            elif m_FileProperties["permission"][pos] == "2":
                                m_PermissionMask = m_PermissionMask + "-w-"
                            elif m_FileProperties["permission"][pos] == "3":
                                m_PermissionMask = m_PermissionMask + "-wx"
                            elif m_FileProperties["permission"][pos] == "4":
                                m_PermissionMask = m_PermissionMask + "r--"
                            elif m_FileProperties["permission"][pos] == "5":
                                m_PermissionMask = m_PermissionMask + "r-x"
                            elif m_FileProperties["permission"][pos] == "6":
                                m_PermissionMask = m_PermissionMask + "rw-"
                            elif m_FileProperties["permission"][pos] == "7":
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
            return None, None, None, None, he.message
