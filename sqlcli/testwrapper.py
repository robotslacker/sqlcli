# -*- coding: utf-8 -*-
import os
import re
import configparser
from .sqlcliexception import SQLCliException


class DiffException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class POSIXCompare:
    CompiledRegexPattern = {}
    ErrorRegexPattern = []

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
                # 如果之前编译过，且没有编译成功，不再尝试编译，直接判断不匹配
                if p_str2 in self.ErrorRegexPattern:
                    return False
                # 正则编译
                if p_compare_ignorecase:
                    m_CompiledPattern = re.compile(p_str2, re.IGNORECASE)
                else:
                    m_CompiledPattern = re.compile(p_str2)
                self.CompiledRegexPattern[p_str2] = m_CompiledPattern
            match_obj = re.match(m_CompiledPattern, p_str1)
            if match_obj is None:
                return False
            elif str(match_obj.group()) != p_str1:
                return False
            else:
                return True
        except re.error:
            # 正则表达式错误，可能是由于这并非是一个正则表达式
            self.ErrorRegexPattern.append(p_str2)
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
        # 将比较文件加载到数组
        file1rawcontent = open(file1, mode='r', encoding=CompareWorkEncoding).readlines()
        reffilerawcontent = open(file2, mode='r', encoding=CompareRefEncoding).readlines()

        workfilecontent = open(file1, mode='r', encoding=CompareWorkEncoding).readlines()
        reffilecontent = open(file2, mode='r', encoding=CompareRefEncoding).readlines()

        lineno1 = []
        lineno2 = []
        for pos in range(0, len(workfilecontent)):
            lineno1.append(pos + 1)
        for pos in range(0, len(reffilecontent)):
            lineno2.append(pos + 1)

        # 去掉filecontent中的回车换行
        for pos in range(0, len(workfilecontent)):
            if workfilecontent[pos].endswith('\n'):
                workfilecontent[pos] = workfilecontent[pos][:-1]
        for pos in range(0, len(reffilecontent)):
            if reffilecontent[pos].endswith('\n'):
                reffilecontent[pos] = reffilecontent[pos][:-1]

        # 去掉fileconent中的首尾空格
        if CompareIgnoreTailOrHeadBlank:
            for pos in range(0, len(workfilecontent)):
                workfilecontent[pos] = workfilecontent[pos].lstrip().rstrip()
            for pos in range(0, len(reffilecontent)):
                reffilecontent[pos] = reffilecontent[pos].lstrip().rstrip()

        # 去除在SkipLine里头的所有内容
        if skiplines is not None:
            pos = 0
            while pos < len(workfilecontent):
                bMatch = False
                for pattern in skiplines:
                    if self.compare_string(workfilecontent[pos], pattern, p_compare_maskEnabled=True):
                        workfilecontent.pop(pos)
                        lineno1.pop(pos)
                        bMatch = True
                        break
                if not bMatch:
                    pos = pos + 1

            pos = 0
            while pos < len(reffilecontent):
                bMatch = False
                for pattern in skiplines:
                    if self.compare_string(reffilecontent[pos], pattern, p_compare_maskEnabled=True):
                        reffilecontent.pop(pos)
                        lineno2.pop(pos)
                        bMatch = True
                        break
                if not bMatch:
                    pos = pos + 1

        # 去除所有的空行
        if CompareIgnoreEmptyLine:
            pos = 0
            while pos < len(workfilecontent):
                if len(workfilecontent[pos].strip()) == 0:
                    workfilecontent.pop(pos)
                    lineno1.pop(pos)
                else:
                    pos = pos + 1
            pos = 0
            while pos < len(reffilecontent):
                if len(reffilecontent[pos].strip()) == 0:
                    reffilecontent.pop(pos)
                    lineno2.pop(pos)
                else:
                    pos = pos + 1

        # 处理MaskLine中的信息，对ref文件进行替换
        if MaskLines is not None:
            pos = 0
            while pos < len(reffilecontent):
                for pattern in MaskLines:
                    if self.compare_string(reffilecontent[pos], pattern, p_compare_maskEnabled=True):
                        reffilecontent[pos] = pattern
                pos = pos + 1

        # 输出两个信息
        # 1：  Compare的结果是否存在dif，True/False
        # 2:   Compare的Dif列表，注意：这里是一个翻转的列表
        (m_CompareResult, m_CompareResultList) = self.compare(workfilecontent, reffilecontent, lineno1, lineno2,
                                                              p_compare_maskEnabled=CompareWithMask,
                                                              p_compare_ignorecase=CompareIgnoreCase)

        # 首先翻转数组
        # 随后从数组中补充进入被Skip掉的内容
        m_nWorkLastPos = 0                                        # 上次Work文件已经遍历到的位置
        m_nRefLastPos = 0                                         # 上次Ref文件已经遍历到的位置
        m_NewCompareResultList = []
        # 从列表中反向开始遍历， Step=-1
        for row in m_CompareResultList[::-1]:
            if row.startswith('+'):
                # 当前日志没有，Refence Log中有的
                # 需要注意的是，Ref文件中被跳过的行不会补充进入dif文件
                m_LineNo = int(row[1:7])
                m_AppendLine = "+{:>{}} ".format(m_LineNo, 6) + reffilerawcontent[m_LineNo-1]
                if m_AppendLine.endswith("\n"):
                    m_AppendLine = m_AppendLine[:-1]
                m_NewCompareResultList.append(m_AppendLine)
                m_nRefLastPos = m_LineNo
                continue
            elif row.startswith('-'):
                # 当前日志有，但是Referencce里头没有的
                m_LineNo = int(row[1:7])
                # 补充填写那些已经被忽略规则略掉的内容，只填写LOG文件中的对应信息
                if m_LineNo > (m_nWorkLastPos + 1):
                    # 当前日志中存在，但是比较的过程中被Skip掉的内容，要首先补充进来
                    for pos in range(m_nWorkLastPos + 1, m_LineNo):
                        m_AppendLine = "S{:>{}} ".format(pos, 6) + file1rawcontent[pos - 1]
                        if m_AppendLine.endswith("\n"):
                            m_AppendLine = m_AppendLine[:-1]
                        m_NewCompareResultList.append(m_AppendLine)
                m_AppendLine = "-{:>{}} ".format(m_LineNo, 6) + file1rawcontent[m_LineNo - 1]
                if m_AppendLine.endswith("\n"):
                    m_AppendLine = m_AppendLine[:-1]
                m_NewCompareResultList.append(m_AppendLine)
                m_nWorkLastPos = m_LineNo
                continue
            elif row.startswith(' '):
                # 两边都有的
                m_LineNo = int(row[0:7])
                # 补充填写那些已经被忽略规则略掉的内容，只填写LOG文件中的对应信息
                if m_LineNo > (m_nWorkLastPos + 1):
                    # 当前日志中存在，但是比较的过程中被Skip掉的内容，要首先补充进来
                    for pos in range(m_nWorkLastPos + 1, m_LineNo):
                        m_AppendLine = "S{:>{}} ".format(pos, 6) + file1rawcontent[pos - 1]
                        if m_AppendLine.endswith("\n"):
                            m_AppendLine = m_AppendLine[:-1]
                        m_NewCompareResultList.append(m_AppendLine)
                # 完全一样的内容
                m_AppendLine = " {:>{}} ".format(m_LineNo, 6) + file1rawcontent[m_LineNo-1]
                if m_AppendLine.endswith("\n"):
                    m_AppendLine = m_AppendLine[:-1]
                m_NewCompareResultList.append(m_AppendLine)
                m_nWorkLastPos = m_LineNo
                m_nRefLastPos = m_nRefLastPos + 1
                continue
            else:
                raise DiffException("Missed line number. Bad compare result. [" + row + "]")

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

    def __init__(self):
        self.SQLOptions = None                # 程序处理参数

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
            for pos in range(0, len(self.c_SkipLines)):
                if self.c_SkipLines[pos] == p_szValue:
                    self.c_SkipLines.pop(pos)
                    break
            return
        if p_szParameter.upper() == "CompareNotMask".upper():
            for pos in range(0, len(self.c_MaskLines)):
                if self.c_MaskLines[pos] == p_szValue:
                    self.c_MaskLines.pop(pos)
                    break
            return
        raise SQLCliException("Invalid parameter [" + str(p_szParameter) + "].")

    def Compare_Files(self, p_szWorkFile, p_szReferenceFile):
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

            # 生成Scenario分析结果
            m_ScenarioStartPos = 0  # 当前Senario开始的位置
            m_ScenarioResults = {}
            m_ScenariosPos = {}

            # 首先记录下来每一个Senario的开始位置，结束位置
            pos = 0
            m_ScenarioName = None
            m_ScenarioPriority = None
            while True:
                if pos >= len(m_CompareResultList):
                    break

                # Scenario定义
                # -- [Hint] setup:
                # -- [setup:]
                # -- [Hint] setup:end:
                # -- [setup:end]

                # -- [Hint] cleanup:
                # -- [cleanup:]
                # -- [Hint] cleanup:end:
                # -- [cleanup:end]

                # -- [Hint] scenario:xxxx:
                # -- [scenario:xxxx]
                # -- [Hint] scenario:priority:xxxx:
                # -- [scenario:priority:xxxx]
                # -- [Hint] scenario:end:
                # -- [scenario:end]

                match_obj = re.search(r"--(\s+)?\[Hint](\s+)?setup:end", m_CompareResultList[pos],
                                     re.IGNORECASE | re.DOTALL)
                if match_obj:
                    if m_ScenarioName is None:
                        m_ScenarioName = "setup"
                    m_ScenariosPos[m_ScenarioName] = {
                        "ScenarioStartPos": m_ScenarioStartPos,
                        "ScenarioEndPos": pos,
                        "ScenarioPriority": m_ScenarioPriority
                    }
                    m_ScenarioStartPos = pos + 1
                    pos = pos + 1
                    m_ScenarioName = None
                    m_ScenarioPriority = None
                    continue

                match_obj = re.search(r"--(\s+)?\[(\s+)?setup:end]", m_CompareResultList[pos],
                                     re.IGNORECASE | re.DOTALL)
                if match_obj:
                    if m_ScenarioName is None:
                        m_ScenarioName = "setup"
                    m_ScenariosPos[m_ScenarioName] = {
                        "ScenarioStartPos": m_ScenarioStartPos,
                        "ScenarioEndPos": pos,
                        "ScenarioPriority": m_ScenarioPriority
                    }
                    m_ScenarioStartPos = pos + 1
                    pos = pos + 1
                    m_ScenarioName = None
                    m_ScenarioPriority = None
                    continue

                match_obj = re.search(r"--(\s+)?\[Hint](\s+)?cleanup:end", m_CompareResultList[pos],
                                     re.IGNORECASE | re.DOTALL)
                if match_obj:
                    if m_ScenarioName is None:
                        m_ScenarioName = "cleanup"
                    m_ScenariosPos[m_ScenarioName] = {
                        "ScenarioStartPos": m_ScenarioStartPos,
                        "ScenarioEndPos": pos,
                        "ScenarioPriority": m_ScenarioPriority
                    }
                    m_ScenarioStartPos = pos + 1
                    pos = pos + 1
                    m_ScenarioName = None
                    m_ScenarioPriority = None
                    continue

                match_obj = re.search(r"--(\s+)?\[(\s+)?cleanup:end]", m_CompareResultList[pos],
                                     re.IGNORECASE | re.DOTALL)
                if match_obj:
                    if m_ScenarioName is None:
                        m_ScenarioName = "cleanup"
                    m_ScenariosPos[m_ScenarioName] = {
                        "ScenarioStartPos": m_ScenarioStartPos,
                        "ScenarioEndPos": pos,
                        "ScenarioPriority": m_ScenarioPriority
                    }
                    m_ScenarioStartPos = pos + 1
                    pos = pos + 1
                    m_ScenarioName = None
                    m_ScenarioPriority = None
                    continue

                match_obj = re.search(r"--(\s+)?\[Hint](\s+)?scenario:end", m_CompareResultList[pos],
                                     re.IGNORECASE | re.DOTALL)
                if match_obj:
                    if m_ScenarioName is None:
                        m_ScenarioName = "none-" + str(m_ScenarioStartPos)
                    m_ScenariosPos[m_ScenarioName] = {
                        "ScenarioStartPos": m_ScenarioStartPos,
                        "ScenarioEndPos": pos,
                        "ScenarioPriority": m_ScenarioPriority
                    }
                    m_ScenarioStartPos = pos + 1
                    pos = pos + 1
                    m_ScenarioName = None
                    m_ScenarioPriority = None
                    continue

                match_obj = re.search(r"--(\s+)?\[(\s+)?scenario:end]", m_CompareResultList[pos],
                                     re.IGNORECASE | re.DOTALL)
                if match_obj:
                    if m_ScenarioName is None:
                        m_ScenarioName = "none-" + str(m_ScenarioStartPos)
                    m_ScenariosPos[m_ScenarioName] = {
                        "ScenarioStartPos": m_ScenarioStartPos,
                        "ScenarioEndPos": pos,
                        "ScenarioPriority": m_ScenarioPriority
                    }
                    m_ScenarioStartPos = pos + 1
                    pos = pos + 1
                    m_ScenarioName = None
                    m_ScenarioPriority = None
                    continue

                match_obj = re.search(r"--(\s+)?\[Hint](\s+)?setup:", m_CompareResultList[pos],
                                     re.IGNORECASE | re.DOTALL)
                if match_obj:
                    m_ScenarioName = "setup"
                    m_ScenarioPriority = None
                    m_ScenarioStartPos = pos
                    pos = pos + 1
                    continue

                match_obj = re.search(r"--(\s+)?\[(\s+)?setup:]", m_CompareResultList[pos],
                                     re.IGNORECASE | re.DOTALL)
                if match_obj:
                    m_ScenarioName = "setup"
                    m_ScenarioPriority = None
                    m_ScenarioStartPos = pos
                    pos = pos + 1
                    continue

                match_obj = re.search(r"--(\s+)?\[Hint](\s+)?cleanup:", m_CompareResultList[pos],
                                     re.IGNORECASE | re.DOTALL)
                if match_obj:
                    m_ScenarioName = "cleanup"
                    m_ScenarioPriority = None
                    m_ScenarioStartPos = pos
                    pos = pos + 1
                    continue

                match_obj = re.search(r"--(\s+)?\[(\s+)?cleanup:]", m_CompareResultList[pos],
                                     re.IGNORECASE | re.DOTALL)
                if match_obj:
                    m_ScenarioName = "cleanup"
                    m_ScenarioPriority = None
                    m_ScenarioStartPos = pos
                    pos = pos + 1
                    continue

                match_obj = re.search(r"--(\s+)?\[Hint](\s+)?Scenario:(.*)", m_CompareResultList[pos],
                                     re.IGNORECASE | re.DOTALL)
                if match_obj:
                    m_SenarioAndPriority = match_obj.group(3).strip()
                    if len(m_SenarioAndPriority.split(':')) == 2:
                        # 重复的Scenario开始
                        if m_ScenarioName is not None and \
                                m_SenarioAndPriority.split(':')[1].strip() == m_ScenarioName:
                            m_ScenarioStartPos = pos
                            pos = pos + 1
                            continue
                    else:
                        # 重复的Scenario开始
                        if m_ScenarioName is not None and \
                                m_ScenarioName == m_SenarioAndPriority:
                            m_ScenarioStartPos = pos
                            pos = pos + 1
                            continue
                    if m_ScenarioName is not None:
                        # 如果上一个Scenario没有正常结束，这里标记结束
                        m_ScenariosPos[m_ScenarioName] = {
                            "ScenarioStartPos": m_ScenarioStartPos,
                            "ScenarioEndPos": pos - 1,
                            "ScenarioPriority": m_ScenarioPriority
                        }
                    if len(m_SenarioAndPriority.split(':')) == 2:
                        # 如果有两个内容， 规则是:Scenario:Priority:ScenarioName
                        m_ScenarioPriority = m_SenarioAndPriority.split(':')[0].strip()
                        m_ScenarioName = m_SenarioAndPriority.split(':')[1].strip()
                    else:
                        # 如果只有一个内容， 规则是:Scenario:ScenarioName
                        m_ScenarioName = m_SenarioAndPriority
                        m_ScenarioPriority = None
                    m_ScenarioStartPos = pos
                    pos = pos + 1
                    continue

                match_obj = re.search(r"--(\s+)?\[(\s+)?Scenario:(.*)]", m_CompareResultList[pos],
                                     re.IGNORECASE | re.DOTALL)
                if match_obj:
                    m_SenarioAndPriority = match_obj.group(3).strip()
                    if len(m_SenarioAndPriority.split(':')) == 2:
                        # 重复的Scenario开始
                        if m_ScenarioName is not None and \
                                m_SenarioAndPriority.split(':')[1].strip() == m_ScenarioName:
                            m_ScenarioStartPos = pos
                            pos = pos + 1
                            continue
                    else:
                        # 重复的Scenario开始
                        if m_ScenarioName is not None and \
                                m_ScenarioName == m_SenarioAndPriority:
                            m_ScenarioStartPos = pos
                            pos = pos + 1
                            continue
                    if m_ScenarioName is not None:
                        # 如果上一个Scenario没有正常结束，这里标记结束
                        m_ScenariosPos[m_ScenarioName] = {
                            "ScenarioStartPos": m_ScenarioStartPos,
                            "ScenarioEndPos": pos - 1,
                            "ScenarioPriority": m_ScenarioPriority
                        }
                    if len(m_SenarioAndPriority.split(':')) == 2:
                        # 如果有两个内容， 规则是:Scenario:Priority:ScenarioName
                        m_ScenarioPriority = m_SenarioAndPriority.split(':')[0].strip()
                        m_ScenarioName = m_SenarioAndPriority.split(':')[1].strip()
                    else:
                        # 如果只有一个内容， 规则是:Scenario:ScenarioName
                        m_ScenarioName = m_SenarioAndPriority
                        m_ScenarioPriority = None
                    m_ScenarioStartPos = pos
                    pos = pos + 1
                    continue

                # 不是什么特殊内容，这里是标准文本
                pos = pos + 1

            # 最后一个Senario的情况记录下来
            if m_ScenarioStartPos < len(m_CompareResultList):
                if m_ScenarioName is not None:
                    m_ScenariosPos[m_ScenarioName] = {
                        "ScenarioStartPos": m_ScenarioStartPos,
                        "ScenarioEndPos": len(m_CompareResultList),
                        "ScenarioPriority": m_ScenarioPriority
                    }
            # 遍历每一个Senario的情况
            for m_ScenarioName, m_Senario_Pos in m_ScenariosPos.items():
                start_pos = m_Senario_Pos['ScenarioStartPos']
                m_EndPos = m_Senario_Pos['ScenarioEndPos']
                m_ScenarioPriority = m_Senario_Pos['ScenarioPriority']
                foundDif = False
                m_DifStartPos = 0
                for pos in range(start_pos, m_EndPos):
                    if m_CompareResultList[pos].startswith('-') or m_CompareResultList[pos].startswith('+'):
                        m_DifStartPos = pos
                        foundDif = True
                        break
                if not foundDif:
                    m_ScenarioResults[m_ScenarioName] = \
                        {
                            "Status": "Successful",
                            "message": "",
                            "Priority": m_ScenarioPriority
                        }
                else:
                    # 错误信息只记录前后20行信息,前5行，多余的不记录
                    if m_DifStartPos - 5 > start_pos:
                        m_DifStartPos = m_DifStartPos - 5
                    else:
                        m_DifStartPos = start_pos
                    if m_DifStartPos + 20 < m_EndPos:
                        m_DifEndPos = m_DifStartPos + 20
                    else:
                        m_DifEndPos = m_EndPos
                    m_Message = "\n".join(m_CompareResultList[m_DifStartPos:m_DifEndPos])
                    m_ScenarioResults[m_ScenarioName] = \
                        {
                            "Status": "FAILURE",
                            "message": m_Message,
                            "Priority": m_ScenarioPriority
                        }

            # 如果没有设置任何Scenario，则将CaseName作为Scenario的名字，统一为一个Scenario
            if len(m_ScenarioResults) == 0:
                if m_CompareResult:
                    m_ScenarioResults["NO-SCENARIO"] = \
                        {
                            "Status": "Successful",
                            "message": "",
                            "Priority": "UNKNOWN"
                        }
                else:
                    m_ScenarioResults["NO-SCENARIO"] = \
                        {
                            "Status": "FAILURE",
                            "message": "Test failed.",
                            "Priority": "UNKNOWN"
                        }

            # 根据优先级过滤Scenario的结果，如果dif发生在不需要运行的Scenario中，则Scenario的结果标记为SKIP
            if self.SQLOptions.get("PRIORITY") != "":
                m_TestPriority = self.SQLOptions.get("PRIORITY")
                m_TestPriorityList = [x.upper().strip() for x in m_TestPriority.split(",")]

                # UNKNOWN的Case总是要运行的，无论如何设置优先级
                for m_ScenarioName, m_ScenarioResult in m_ScenarioResults.items():
                    if m_ScenarioResult["Status"] == "FAILURE" and \
                            m_ScenarioResult["Priority"] is not None and \
                            m_ScenarioResult["Priority"] not in m_TestPriorityList:
                        m_ScenarioResults[m_ScenarioName] = \
                            {
                                "Status": "SKIP",
                                "message": "Skip this scenario due to env TEST_PRIORITY set.",
                                "Priority": m_ScenarioResult["Priority"]
                            }
                        for m_linePos in range(
                                m_ScenariosPos[m_ScenarioName]["ScenarioStartPos"],
                                m_ScenariosPos[m_ScenarioName]["ScenarioEndPos"] + 1):
                            # 对于不需要展示的Scenario, 第一行标记Scenario的名字，其他行过滤删除
                            if m_linePos == m_ScenariosPos[m_ScenarioName]["ScenarioStartPos"]:
                                m_CompareResultList[m_linePos] = \
                                    "S" + m_CompareResultList[m_linePos][1:8] + \
                                    "Scenario [" + m_ScenarioName + "] skipped due to priority limit."
                            else:
                                if m_linePos < len(m_CompareResultList):
                                    m_CompareResultList[m_linePos] = "!@#$%^&*"
                # 过滤掉所有不需要展现的，已经被SKIP的内容
                m_CompareResultList = [x for x in m_CompareResultList if x != "!@#$%^&*"]

            # 遍历所有Scneario的结果，如果全部为SUCCESSFUL，则Case为成功，否则为失败
            m_CompareResult = True
            for m_LineItem in m_CompareResultList:
                if m_LineItem.startswith('-') or m_LineItem.startswith('+'):
                    m_CompareResult = False
                    break
            if m_CompareResult:
                for m_ScenarioResult in m_ScenarioResults.values():
                    if m_ScenarioResult["Status"] == "FAILURE":
                        m_CompareResult = False
                        break

            # 记录Report文件的位置信息, 默认生成在WorkFilePath下
            (m_WorkFilePath, m_TempFileName) = os.path.split(os.path.abspath(p_szWorkFile))
            (m_ShortWorkFileName, m_WorkFileExtension) = os.path.splitext(m_TempFileName)
            m_DifFileName = m_ShortWorkFileName + '.dif'
            m_SucFileName = m_ShortWorkFileName + '.suc'
            m_xlogFileName = m_ShortWorkFileName + '.xlog'
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
                m_Result = "Compare Failed! " + \
                           "Please check [" + os.path.abspath(m_DifFullFileName) + "] for more information."

            # 返回结果
            m_Headers = ["Scenario", "Priority", "Status", ]
            m_ReturnResult = []
            m_ScenarioCount = 0
            m_ScenarioSuccCount = 0
            m_ScenarioSkipCount = 0
            for m_ScenarioName, m_ScenarioResult in m_ScenarioResults.items():
                m_ReturnResult.append(
                    [m_ScenarioName, m_ScenarioResult["Priority"], m_ScenarioResult["Status"]]
                )
                m_ScenarioCount = m_ScenarioCount + 1
                if m_ScenarioResult["Status"] == "Successful":
                    m_ScenarioSuccCount = m_ScenarioSuccCount + 1
                if m_ScenarioResult["Status"] == "SKIP":
                    m_ScenarioSkipCount = m_ScenarioSkipCount + 1
            m_Result = \
                str(m_ScenarioSuccCount) + " successful with total " + \
                str(m_ScenarioCount) + "(" + str(m_ScenarioSkipCount) + " kipped) scenarios.\n" + m_Result
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
                return None, None, None, None, "Assert Failed."
        except SyntaxError:
            return None, None, None, None, "Assert Failed."
        except Exception as ae:
            raise SQLCliException(str(ae))

    @staticmethod
    def LoadEnv(p_EnvFileName, p_EnvSectionName):
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

        match_obj = re.match(r"test\s+set\s+(.*?)\s+(.*?)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_Parameter = match_obj.group(1).strip()
            m_Value = match_obj.group(2).strip()
            self.setTestOptions(m_Parameter, m_Value)
            return None, None, None, None, "set successful."

        match_obj = re.match(r"test\s+compare\s+(.*)\s+with\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_WorkFile = match_obj.group(1).strip()
            m_RefFile = match_obj.group(2).strip()
            if m_WorkFile.startswith("'") and m_WorkFile.endswith("'"):
                m_WorkFile = m_WorkFile[1:-1]
            if m_RefFile.startswith("'") and m_RefFile.endswith("'"):
                m_RefFile = m_RefFile[1:-1]
            (title, result, headers, columnTypes, status) = self.Compare_Files(m_WorkFile, m_RefFile)
            return title, result, headers, columnTypes, status

        match_obj = re.match(r"test\s+assert\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_formular = match_obj.group(1).strip()
            (title, result, headers, columnTypes, status) = self.AssertFormular(m_formular)
            return title, result, headers, columnTypes, status

        match_obj = re.match(r"test\s+loadenv\s+(.*)\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_EnvFileName = match_obj.group(1).strip()
            m_EnvSectionName = match_obj.group(2).strip()
            (title, result, headers, columnTypes, status) = self.LoadEnv(m_EnvFileName, m_EnvSectionName)
            return title, result, headers, columnTypes, status

        return None, None, None, None, "Unknown test Command."
