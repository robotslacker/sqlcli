# -*- coding: utf-8 -*-
import os
import re
import json
import configparser
from .sqlcliexception import SQLCliException


class DiffException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class POSIXCompare:
    CompiledRegexPattern = {}

    # 正则表达比较两个字符串
    # p_str1                  原字符串
    # p_str2                  正则表达式
    # p_compare_maskEnabled   是否按照正则表达式来判断是否相等
    # p_compare_ignorecase    是否忽略匹配中的大小写
    def compare_string(self, p_str1, p_str2,
                       p_compare_maskEnabled=False,
                       p_compare_ignorecase=False):
        # 如果两个字符串完全相等，直接返回
        if p_str1 == p_str2:
            return True

        # 如果忽略大小写的情况下，两个字符串的大写相同，则直接返回
        if p_compare_ignorecase:
            if p_str1.upper() == p_str2.upper():
                return True

        # 如果没有启用正则，则直接返回不相等
        if not p_compare_maskEnabled:
            return False

        # 用正则判断表达式是否相等
        try:
            if p_str2 in self.CompiledRegexPattern:
                m_CompiledPattern = self.CompiledRegexPattern[p_str2]
            else:
                m_CompiledPattern = re.compile(p_str2)
                self.CompiledRegexPattern[p_str2] = m_CompiledPattern
            if p_compare_ignorecase:
                matchObj = re.match(m_CompiledPattern, p_str1, re.IGNORECASE)
            else:
                matchObj = re.match(m_CompiledPattern, p_str1)
            if matchObj is None:
                return False
            elif str(matchObj.group()) != p_str1:
                return False
            else:
                return True
        except re.error:
            # 正则表达式错误，可能是由于这并非是一个正则表达式
            return False

    def compare(self,
                x,
                y,
                linenox,
                linenoy,
                p_compare_maskEnabled=False,
                p_compare_ignorecase=False):
        # LCS问题就是求两个字符串最长公共子串的问题。
        # 解法就是用一个矩阵来记录两个字符串中所有位置的两个字符之间的匹配情况，若是匹配则为1，否则为0。
        # 然后求出对角线最长的1序列，其对应的位置就是最长匹配子串的位置。

        # c           LCS数组
        # x           源数据
        # y           目的数据
        # linenox     源数据行数信息
        # linenoy     目的数据行数信息
        # i           源数据长度
        # j           目的数据长度
        # p_result    比对结果
        next_x = x
        next_y = y
        next_i = len(x) - 1
        next_j = len(y) - 1

        # This is our matrix comprised of list of lists.
        # We allocate extra row and column with zeroes for the base case of empty
        # sequence. Extra row and column is appended to the end and exploit
        # Python's ability of negative indices: x[-1] is the last elem.
        # 构建LCS数组
        c = [[0 for _ in range(len(y) + 1)] for _ in range(len(x) + 1)]
        for i, xi in enumerate(x):
            for j, yj in enumerate(y):
                if self.compare_string(xi, yj,
                                       p_compare_maskEnabled, p_compare_ignorecase):
                    c[i][j] = 1 + c[i - 1][j - 1]
                else:
                    c[i][j] = max(c[i][j - 1], c[i - 1][j])

        # 开始比较
        compare_result = True
        m_CompareDiffResult = []
        while True:
            if next_i < 0 and next_j < 0:
                break
            elif next_i < 0:
                compare_result = False
                m_CompareDiffResult.append("+{:>{}} ".format(linenoy[next_j], 6) + next_y[next_j])
                next_j = next_j - 1
            elif next_j < 0:
                compare_result = False
                m_CompareDiffResult.append("-{:>{}} ".format(linenox[next_i], 6) + next_x[next_i])
                next_i = next_i - 1
            elif self.compare_string(next_x[next_i], next_y[next_j],
                                     p_compare_maskEnabled, p_compare_ignorecase):
                m_CompareDiffResult.append(" {:>{}} ".format(linenox[next_i], 6) + next_x[next_i])
                next_i = next_i - 1
                next_j = next_j - 1
            elif c[next_i][next_j - 1] >= c[next_i - 1][next_j]:
                compare_result = False
                m_CompareDiffResult.append("+{:>{}} ".format(linenoy[next_j], 6) + next_y[next_j])
                next_j = next_j - 1
            elif c[next_i][next_j - 1] < c[next_i - 1][next_j]:
                compare_result = False
                m_CompareDiffResult.append("-{:>{}} ".format(linenox[next_i], 6) + next_x[next_i])
                next_i = next_i - 1
        return compare_result, m_CompareDiffResult

    def compare_text_files(self, file1, file2,
                           skiplines=None,
                           MaskLines=None,
                           CompareIgnoreEmptyLine=False,
                           CompareWithMask=None,
                           CompareIgnoreCase=False,
                           CompareIgnoreTailOrHeadBlank=False,
                           CompareWorkEncoding='UTF-8',
                           CompareRefEncoding='UTF-8'):
        """
            Return:
                CompareResult           True/False   是否比对成功
                CompareResultList       Compare比较结果
                   - 开头的内容为工作文件缺失，但是参考文件中存在
                   + 开头的内容为工作文件多出，但是参考文件中确实
                   S 开头的内容为由于配置规则导致改行被忽略
        """
        # 将比较文件加载到数组
        try:
            workfilerawcontent = open(file1, mode='r', encoding=CompareWorkEncoding).readlines()
            workfilecontent = open(file1, mode='r', encoding=CompareWorkEncoding).readlines()
        except UnicodeDecodeError:
            raise DiffException("UnicodeDecodeError('" + CompareWorkEncoding + "') for [" + file1 + "]")
        try:
            reffilecontent = open(file2, mode='r', encoding=CompareRefEncoding).readlines()
        except UnicodeDecodeError:
            raise DiffException("UnicodeDecodeError('" + CompareRefEncoding + "') for [" + file2 + "]")

        lineno1 = []
        lineno2 = []
        for m_nPos in range(0, len(workfilecontent)):
            lineno1.append(m_nPos + 1)
        for m_nPos in range(0, len(reffilecontent)):
            lineno2.append(m_nPos + 1)

        # 去掉filecontent中的回车换行
        for m_nPos in range(0, len(workfilecontent)):
            if workfilecontent[m_nPos].endswith('\n'):
                workfilecontent[m_nPos] = workfilecontent[m_nPos][:-1]
        for m_nPos in range(0, len(reffilecontent)):
            if reffilecontent[m_nPos].endswith('\n'):
                reffilecontent[m_nPos] = reffilecontent[m_nPos][:-1]

        # 去掉fileconent中的首尾空格
        if CompareIgnoreTailOrHeadBlank:
            for m_nPos in range(0, len(workfilecontent)):
                workfilecontent[m_nPos] = workfilecontent[m_nPos].lstrip().rstrip()
            for m_nPos in range(0, len(reffilecontent)):
                reffilecontent[m_nPos] = reffilecontent[m_nPos].lstrip().rstrip()

        # 去除在SkipLine里头的所有内容
        if skiplines is not None:
            m_nPos = 0
            while m_nPos < len(workfilecontent):
                bMatch = False
                for pattern in skiplines:
                    if self.compare_string(workfilecontent[m_nPos], pattern, p_compare_maskEnabled=True):
                        workfilecontent.pop(m_nPos)
                        lineno1.pop(m_nPos)
                        bMatch = True
                        break
                if not bMatch:
                    m_nPos = m_nPos + 1

            m_nPos = 0
            while m_nPos < len(reffilecontent):
                bMatch = False
                for pattern in skiplines:
                    if self.compare_string(reffilecontent[m_nPos], pattern, p_compare_maskEnabled=True):
                        reffilecontent.pop(m_nPos)
                        lineno2.pop(m_nPos)
                        bMatch = True
                        break
                if not bMatch:
                    m_nPos = m_nPos + 1

        # 去除所有的空行
        if CompareIgnoreEmptyLine:
            m_nPos = 0
            while m_nPos < len(workfilecontent):
                if len(workfilecontent[m_nPos].strip()) == 0:
                    workfilecontent.pop(m_nPos)
                    lineno1.pop(m_nPos)
                else:
                    m_nPos = m_nPos + 1
            m_nPos = 0
            while m_nPos < len(reffilecontent):
                if len(reffilecontent[m_nPos].strip()) == 0:
                    reffilecontent.pop(m_nPos)
                    lineno2.pop(m_nPos)
                else:
                    m_nPos = m_nPos + 1

        # 处理MaskLine中的信息，对ref文件进行替换
        if MaskLines is not None:
            m_nPos = 0
            while m_nPos < len(reffilecontent):
                for pattern in MaskLines:
                    if self.compare_string(reffilecontent[m_nPos], pattern, p_compare_maskEnabled=True):
                        reffilecontent[m_nPos] = pattern
                m_nPos = m_nPos + 1

        # 输出两个信息
        # 1：  Compare的结果是否存在dif，True/False
        # 2:   Compare的Dif列表，注意：这里是一个翻转的列表
        (m_CompareResult, m_CompareResultList) = self.compare(workfilecontent, reffilecontent, lineno1, lineno2,
                                                              p_compare_maskEnabled=CompareWithMask,
                                                              p_compare_ignorecase=CompareIgnoreCase)
        # 首先翻转数组
        # 随后从数组中补充进入被Skip掉的内容
        m_nLastPos = 0
        m_NewCompareResultList = []
        # 从列表中反向开始遍历， Step=-1
        for row in m_CompareResultList[::-1]:
            if row.startswith('+'):
                # 当前日志没有，Log中有的，忽略不计
                m_NewCompareResultList.append(row)
                continue
            elif row.startswith('-'):
                # 记录当前行号
                m_LineNo = int(row[1:7])
            elif row.startswith(' '):
                # 记录当前行号
                m_LineNo = int(row[0:7])
            else:
                raise DiffException("Missed line number. Bad compare result. [" + row + "]")
            if m_LineNo > (m_nLastPos + 1):
                for m_nPos in range(m_nLastPos + 1, m_LineNo):
                    m_AppendLine = "S{:>{}} ".format(m_nPos, 6) + workfilerawcontent[m_nPos - 1]
                    if m_AppendLine.endswith("\n"):
                        m_AppendLine = m_AppendLine[:-1]
                    m_NewCompareResultList.append(m_AppendLine)
                m_NewCompareResultList.append(row)
                m_nLastPos = m_LineNo
            else:
                m_NewCompareResultList.append(row)
                m_nLastPos = m_LineNo
        return m_CompareResult, m_NewCompareResultList


class TestWrapper(object):
    c_SkipLines = []
    c_MaskLines = []                          # Compare比较的时候掩码相关内容
    c_IgnoreEmptyLine = True                  # 是否在比对的时候忽略空白行
    c_CompareEnableMask = True                # 是否在比对的时候利用正则表达式
    c_CompareIgnoreCase = False               # 是否再比对的时候忽略大小写
    c_CompareIgnoreTailOrHeadBlank = False    # 是否忽略对比的前后空格
    c_CompareWorkEncoding = 'UTF-8'           # Compare工作文件字符集
    c_CompareResultEncoding = 'UTF-8'         # Compare结果文件字符集
    c_CompareRefEncoding = 'UTF-8'            # Compare参考文件字符集
    c_CompareReportDetailMode = False         # Compare日志显示样式， 是否显示详细内容
    c_CompareGenerateReport = False           # 是否在本地生成Dif/Suc/xLog文件报告
    c_CompareGenerateReportDir = None         # 本地Dif/Suc/xLog报告的生成目录

    def setTestOptions(self, p_szParameter, p_szValue):
        if p_szParameter.upper() == "IgnoreEmptyLine".upper():
            if p_szValue.upper() == "TRUE":
                self.c_IgnoreEmptyLine = True
            elif p_szValue.upper() == "FALSE":
                self.c_IgnoreEmptyLine = False
            else:
                raise SQLCliException("Invalid option value [" + str(p_szValue) + "]. True/False only.")
            return
        if p_szParameter.upper() == "CompareEnableMask".upper():
            if p_szValue.upper() == "TRUE":
                self.c_CompareEnableMask = True
            elif p_szValue.upper() == "FALSE":
                self.c_CompareEnableMask = False
            else:
                raise SQLCliException("Invalid option value [" + str(p_szValue) + "]. True/False only.")
            return
        if p_szParameter.upper() == "CompareIgnoreCase".upper():
            if p_szValue.upper() == "TRUE":
                self.c_CompareIgnoreCase = True
            elif p_szValue.upper() == "FALSE":
                self.c_CompareIgnoreCase = False
            else:
                raise SQLCliException("Invalid option value [" + str(p_szValue) + "]. True/False only.")
            return
        if p_szParameter.upper() == "CompareIgnoreTailOrHeadBlank".upper():
            if p_szValue.upper() == "TRUE":
                self.c_CompareIgnoreTailOrHeadBlank = True
            elif p_szValue.upper() == "FALSE":
                self.c_CompareIgnoreTailOrHeadBlank = False
            else:
                raise SQLCliException("Invalid option value [" + str(p_szValue) + "]. True/False only.")
            return
        if p_szParameter.upper() == "CompareWorkEncoding".upper():
            self.c_CompareWorkEncoding = p_szValue
            return
        if p_szParameter.upper() == "CompareResultEncoding".upper():
            self.c_CompareResultEncoding = p_szValue
            return
        if p_szParameter.upper() == "CompareRefEncoding".upper():
            self.c_CompareRefEncoding = p_szValue
            return
        if p_szParameter.upper() == "CompareSkip".upper():
            m_SkipPatternExisted = False
            for m_SkipLine in self.c_SkipLines:
                if m_SkipLine == p_szValue:
                    m_SkipPatternExisted = True
                    break
            if not m_SkipPatternExisted:
                self.c_SkipLines.append(p_szValue)
            return
        if p_szParameter.upper() == "CompareMask".upper():
            m_MaskPatternExisted = False
            for m_MaskLine in self.c_MaskLines:
                if m_MaskLine == p_szValue:
                    m_MaskPatternExisted = True
                    break
            if not m_MaskPatternExisted:
                self.c_MaskLines.append(p_szValue)
            return

        if p_szParameter.upper() == "CompareNotSkip".upper():
            for m_nPos in range(0, len(self.c_SkipLines)):
                if self.c_SkipLines[m_nPos] == p_szValue:
                    self.c_SkipLines.pop(m_nPos)
                    break
            return
        if p_szParameter.upper() == "CompareNotMask".upper():
            for m_nPos in range(0, len(self.c_MaskLines)):
                if self.c_MaskLines[m_nPos] == p_szValue:
                    self.c_MaskLines.pop(m_nPos)
                    break
            return
        if p_szParameter.upper() == "CompareReportDetailMode".upper():
            if p_szValue.upper() == "TRUE":
                self.c_CompareReportDetailMode = True
            elif p_szValue.upper() == "FALSE":
                self.c_CompareReportDetailMode = False
            else:
                raise SQLCliException("Invalid option value [" + str(p_szValue) + "]. True/False only.")
            return
        if p_szParameter.upper() == "CompareGenerateReport".upper():
            if p_szValue.upper() == "TRUE":
                self.c_CompareGenerateReport = True
            elif p_szValue.upper() == "FALSE":
                self.c_CompareGenerateReport = False
            else:
                raise SQLCliException("Invalid option value [" + str(p_szValue) + "]. True/False only.")
            return
        if p_szParameter.upper() == "CompareGenerateReportDir".upper():
            self.c_CompareGenerateReportDir = p_szValue
            return

        raise SQLCliException("Invalid parameter [" + str(p_szParameter) + "].")

    def Compare_Files(self, p_szWorkFile, p_szReferenceFile):
        m_Title = None
        if self.c_CompareReportDetailMode:
            m_Title = "Compare text files:"
            m_Title = m_Title + "\n" + "  Workfile:          [" + str(p_szWorkFile) + "]"
            m_Title = m_Title + "\n" + "  Reffile:           [" + str(p_szReferenceFile) + "]"

        # 检查文件是否存在
        if not os.path.isfile(p_szWorkFile):
            m_Result = "Compare failed! Work log ['" + p_szWorkFile + "'] does not exist!"
            return m_Title, None, None, None, m_Result
        if not os.path.isfile(p_szReferenceFile):
            m_Result = "Compare failed! Reference log ['" + p_szReferenceFile + "'] does not exist!"
            return m_Title, None, None, None, m_Result

        # 比对文件
        m_Comparer = POSIXCompare()
        try:
            # 这里的CompareResultList是一个被翻转了的列表，在输出的时候，需要翻转回来
            (m_CompareResult, m_CompareResultList) = m_Comparer.compare_text_files(
                file1=p_szWorkFile,
                file2=p_szReferenceFile,
                skiplines=self.c_SkipLines,
                MaskLines=self.c_MaskLines,
                CompareIgnoreEmptyLine=self.c_IgnoreEmptyLine,
                CompareWithMask=self.c_CompareEnableMask,
                CompareIgnoreCase=self.c_CompareIgnoreCase,
                CompareIgnoreTailOrHeadBlank=self.c_CompareIgnoreTailOrHeadBlank,
                CompareWorkEncoding=self.c_CompareWorkEncoding,
                CompareRefEncoding=self.c_CompareRefEncoding,
            )

            # 记录Report文件的位置信息
            (m_WorkFilePath, m_TempFileName) = os.path.split(p_szWorkFile)
            (m_ShortWorkFileName, m_WorkFileExtension) = os.path.splitext(m_TempFileName)
            m_DifFileName = m_ShortWorkFileName + '.dif'
            m_SucFileName = m_ShortWorkFileName + '.suc'
            m_xlogFileName = m_ShortWorkFileName + '.xlog'
            if self.c_CompareGenerateReportDir is not None:
                m_WorkFilePath = self.c_CompareGenerateReportDir
            else:
                m_WorkFilePath = os.getcwd()  # 默认当前目录
            m_DifFullFileName = os.path.join(m_WorkFilePath, m_DifFileName)
            m_xlogFullFileName = os.path.join(m_WorkFilePath, m_xlogFileName)
            m_SucFullFileName = os.path.join(m_WorkFilePath, m_SucFileName)
            # 尝试先删除旧的数据文件
            if os.path.exists(m_DifFullFileName):
                os.remove(m_DifFullFileName)
            if os.path.exists(m_SucFullFileName):
                os.remove(m_SucFullFileName)
            if os.path.exists(m_xlogFullFileName):
                os.remove(m_xlogFullFileName)

            if m_CompareResult:
                if self.c_CompareGenerateReport:
                    m_Title = m_Title + "\n" + "  Sucfile:           [" + str(m_SucFullFileName) + "]"
                    m_Title = m_Title + "\n" + "  xLogfile:          [" + str(m_xlogFullFileName) + "]"
                    m_Title = m_Title + "\n" + "  Mask flag:         [" + str(self.c_CompareEnableMask) + "]"
                    m_Title = m_Title + "\n" + "  BlankSpace flag:   [" + str(self.c_CompareIgnoreTailOrHeadBlank) + "]"
                    m_Title = m_Title + "\n" + "  CaseSentive flag:  [" + str(self.c_CompareIgnoreCase) + "]"
                    m_Title = m_Title + "\n" + "  Empty line flag:   [" + str(self.c_IgnoreEmptyLine) + "]"
                    for row in self.c_SkipLines:
                        m_Title = m_Title + "\n" + "  Skip line..:       [" + str(row) + "]"
                    for row in self.c_MaskLines:
                        m_Title = m_Title + "\n" + "  Mask line..:       [" + str(row) + "]"
                # 生成一个空的suc文件
                m_CompareResultFile = open(m_SucFullFileName, 'w', encoding=self.c_CompareResultEncoding)
                m_CompareResultFile.close()
                # 返回比对结果
                m_Result = "Compare Successful!"
            else:
                if self.c_CompareGenerateReport:
                    m_Title = m_Title + "\n" + "  Diffile:           [" + str(m_DifFullFileName) + "]"
                    m_Title = m_Title + "\n" + "  xLogfile:          [" + str(m_xlogFullFileName) + "]"
                    m_Title = m_Title + "\n" + "  Mask flag:         [" + str(self.c_CompareEnableMask) + "]"
                    m_Title = m_Title + "\n" + "  BlankSpace flag:   [" + str(self.c_CompareIgnoreTailOrHeadBlank) + "]"
                    m_Title = m_Title + "\n" + "  CaseSentive flag:  [" + str(self.c_CompareIgnoreCase) + "]"
                    m_Title = m_Title + "\n" + "  Empty line flag:   [" + str(self.c_IgnoreEmptyLine) + "]"
                    for row in self.c_SkipLines:
                        m_Title = m_Title + "\n" + "  Skip line..:       [" + str(row) + "]"
                    for row in self.c_MaskLines:
                        m_Title = m_Title + "\n" + "  Mask line..:       [" + str(row) + "]"
                # 生成dif文件
                m_CompareResultFile = open(m_DifFullFileName, 'w', encoding=self.c_CompareResultEncoding)
                for line in m_CompareResultList:
                    print(line, file=m_CompareResultFile)
                m_CompareResultFile.close()
                # 返回比对结果
                if self.c_CompareGenerateReport:
                    m_Result = "Compare Failed! Please check report files for more information."
                else:
                    m_Result = "Compare Failed!"

            # 用于Scenario分析
            m_ScenarioName = "NONE-0"
            m_ScenarioStartPos = 0
            m_nPos = 0
            m_ScenariosStatus = {}             # 记录成功或者失败，用来在命令行中的返回结果
            m_ScenariosResults = {}            # 记录详细的失败消息，用于生成xlog文件
            m_bSaveResult = False
            while True:
                if m_nPos >= len(m_CompareResultList):
                    break
                matchObj = re.search(r"--(\s+)?\[Hint](\s+)?setup:", m_CompareResultList[m_nPos],
                                     re.IGNORECASE | re.DOTALL)
                if matchObj:
                    if not m_bSaveResult:
                        if not m_ScenarioName.startswith("NONE"):
                            m_ScenariosResults[m_ScenarioName] = "Successful"
                    m_ScenarioName = "setup"
                    m_ScenarioStartPos = m_nPos
                    m_nPos = m_nPos + 1
                    continue

                matchObj = re.search(r"--(\s+)?\[Hint](\s+)?cleanup:", m_CompareResultList[m_nPos],
                                     re.IGNORECASE | re.DOTALL)
                if matchObj:
                    if not m_bSaveResult:
                        if not m_ScenarioName.startswith("NONE"):
                            m_ScenariosResults[m_ScenarioName] = "Successful"
                    m_ScenarioName = "cleanup"
                    m_ScenarioStartPos = m_nPos
                    m_nPos = m_nPos + 1
                    continue

                matchObj = re.search(r"--(\s+)?\[Hint](\s+)?setup:end", m_CompareResultList[m_nPos],
                                     re.IGNORECASE | re.DOTALL)
                if matchObj:
                    if not m_bSaveResult:
                        if not m_ScenarioName.startswith("NONE"):
                            m_ScenariosResults[m_ScenarioName] = "Successful"
                    m_ScenarioName = "NONE-" + str(m_nPos)
                    m_bSaveResult = False
                    m_ScenarioStartPos = 0
                    m_nPos = m_nPos + 1
                    continue

                matchObj = re.search(r"--(\s+)?\[Hint](\s+)?cleanup:end", m_CompareResultList[m_nPos],
                                     re.IGNORECASE | re.DOTALL)
                if matchObj:
                    if not m_bSaveResult:
                        if not m_ScenarioName.startswith("NONE"):
                            m_ScenariosResults[m_ScenarioName] = "Successful"
                    m_ScenarioName = "NONE-" + str(m_nPos)
                    m_bSaveResult = False
                    m_ScenarioStartPos = 0
                    m_nPos = m_nPos + 1
                    continue

                matchObj = re.search(r"--(\s+)?\[Hint](\s+)?Scenario:(.*)", m_CompareResultList[m_nPos],
                                     re.IGNORECASE | re.DOTALL)
                if matchObj:
                    if not m_bSaveResult:
                        if not m_ScenarioName.startswith("NONE"):
                            m_ScenariosResults[m_ScenarioName] = "Successful"
                    m_ScenarioName = matchObj.group(3).strip()
                    m_ScenarioStartPos = m_nPos
                    if m_ScenarioName.upper() == "END":
                        m_ScenarioName = "NONE-" + str(m_nPos)
                        m_bSaveResult = False
                        m_ScenarioStartPos = 0
                    m_nPos = m_nPos + 1
                    continue

                # 错误信息只记录前后20行信息,前5行，多余的不记录
                if m_CompareResultList[m_nPos].startswith('-') or m_CompareResultList[m_nPos].startswith('+'):
                    if m_nPos < 5:
                        m_nStartPos = 0
                    else:
                        m_nStartPos = m_nPos - 5
                    if m_nStartPos < m_ScenarioStartPos:
                        m_nStartPos = m_ScenarioStartPos
                    if m_nStartPos + 20 > len(m_CompareResultList):
                        m_nEndPos = len(m_CompareResultList)
                    else:
                        m_nEndPos = m_nStartPos + 20
                    m_szErrorMessage = m_CompareResultList[m_nStartPos] + "\n"
                    m_nPos2 = m_nStartPos + 1
                    while True:
                        # 本次记录dif的信息从本次Scenario开始的地方记录，截止到下一个Scenarios开始或本次Scneario终止
                        if m_nPos2 >= len(m_CompareResultList):
                            break
                        matchObj = re.search(r"--(\s+)?\[Hint](\s+)?Scenario:(.*)", m_CompareResultList[m_nPos2],
                                             re.IGNORECASE | re.DOTALL)
                        if matchObj:
                            break  # Scenario终止
                        matchObj = re.search(r"--(\s+)?\[(\s+)?Scenario:(.*)]", m_CompareResultList[m_nPos2],
                                             re.IGNORECASE | re.DOTALL)
                        if matchObj:
                            break  # Scenario终止
                        if m_nPos2 < m_nEndPos:
                            m_szErrorMessage = m_szErrorMessage + m_CompareResultList[m_nPos2] + "\n"
                        else:
                            break  # 记录已经超过了5+20行
                        m_nPos2 = m_nPos2 + 1
                    m_nPos = m_nPos + 1
                    m_bSaveResult = True
                    m_ScenariosStatus[m_ScenarioName] = 'Failed'
                    m_ScenariosResults[m_ScenarioName] = m_szErrorMessage
                else:
                    m_nPos = m_nPos + 1
            if not m_bSaveResult:
                m_ScenariosResults[m_ScenarioName] = "Successful"
                m_ScenariosStatus[m_ScenarioName] = 'Successful'
            if self.c_CompareGenerateReport:
                m_xlogResults = {"ScenarioResults": m_ScenariosResults}
                with open(m_xlogFullFileName, 'w', encoding=self.c_CompareResultEncoding) as f:
                    json.dump(m_xlogResults, f)

            m_Headers = ["Scenario", "Result"]
            m_ReturnResult = []
            for m_ScenarioName, m_ScenarioStatus in m_ScenariosStatus.items():
                if self.c_CompareReportDetailMode:
                    if m_ScenarioStatus == "Failed":
                        m_Result = m_Result + "\n... >>>>>>> ...\n" + "Scenario:[" + m_ScenarioName + "]"
                        m_Result = m_Result + "\n" + m_ScenariosResults[m_ScenarioName]
                m_ReturnResult.append([m_ScenarioName, m_ScenarioStatus])
            return m_Title, m_ReturnResult, m_Headers, None, m_Result
        except DiffException as de:
            raise SQLCliException('Fatal Diff Exception:: ' + de.message)

    def AssertFormular(self, p_Formular):
        if self:
            pass
        try:
            if eval(str(p_Formular)):
                return None, None, None, None, "Assert Successful."
            else:
                raise SQLCliException('Assert Failed.')
        except Exception as ae:
            raise SQLCliException('Assert Error: ' + repr(ae))

    def LoadEnv(self, p_EnvFileName, p_EnvSectionName):
        if not os.path.exists(p_EnvFileName):
            raise SQLCliException("Env file [" + p_EnvFileName + "] does not exist!")

        m_EnvSettings = configparser.ConfigParser()
        m_EnvSettings.optionxform = str
        m_EnvSettings.read(p_EnvFileName)
        if not m_EnvSettings.has_section(p_EnvSectionName):
            raise SQLCliException(
                "Section [" + p_EnvSectionName + "] does not exist in file [" + p_EnvFileName + "]!")

        for m_ConfigName, m_ConfigValue in m_EnvSettings.items(p_EnvSectionName):
            os.environ[m_ConfigName.strip()] = m_ConfigValue.strip()
            if "SQLCLI_DEBUG" in os.environ:
                print("Load Env :: " + m_ConfigName + "=" + m_ConfigValue)
        return None, None, None, None, "Env [" + p_EnvSectionName + "] load successful."

    def Process_SQLCommand(self, p_szSQL):
        m_szSQL = p_szSQL.strip()

        matchObj = re.match(r"test\s+set\s+(.*?)\s+(.*?)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_Parameter = matchObj.group(1).strip()
            m_Value = matchObj.group(2).strip()
            self.setTestOptions(m_Parameter, m_Value)
            return None, None, None, None, "set successful."

        matchObj = re.match(r"test\s+compare\s+(.*)\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_WorkFile = matchObj.group(1).strip()
            m_RefFile = matchObj.group(2).strip()
            (title, result, headers, columntypes, status) = self.Compare_Files(m_WorkFile, m_RefFile)
            return title, result, headers, columntypes, status

        matchObj = re.match(r"test\s+assert\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_formular = matchObj.group(1).strip()
            (title, result, headers, columntypes, status) = self.AssertFormular(m_formular)
            return title, result, headers, columntypes, status

        matchObj = re.match(r"test\s+loadenv\s+(.*)\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_EnvFileName = matchObj.group(1).strip()
            m_EnvSectionName = matchObj.group(2).strip()
            (title, result, headers, columntypes, status) = self.LoadEnv(m_EnvFileName, m_EnvSectionName)
            return title, result, headers, columntypes, status

        return None, None, None, None, "Unknown test Command."
