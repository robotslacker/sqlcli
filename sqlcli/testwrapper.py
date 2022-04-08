# -*- coding: utf-8 -*-
import os
import re
import configparser
from collections import namedtuple
from .sqlcliexception import SQLCliException


class DiffException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class POSIXCompare:
    CompiledRegexPattern = {}
    ErrorRegexPattern = []

    # These define the structure of the history, and correspond to diff output with
    # lines that start with a space, a + and a - respectively.
    Keep = namedtuple('Keep', ['line', 'lineno'])
    Insert = namedtuple('Insert', ['line', 'lineno'])
    Remove = namedtuple('Remove', ['line', 'lineno'])
    # See frontier in myers_diff
    Frontier = namedtuple('Frontier', ['x', 'history'])

    # 正则表达比较两个字符串
    # p_str1                  原字符串
    # p_str2                  正则表达式
    # p_compare_maskEnabled   是否按照正则表达式来判断是否相等
    # p_compare_ignoreCase    是否忽略匹配中的大小写
    def compare_string(self, p_str1, p_str2,
                       p_compare_maskEnabled=False,
                       p_compare_ignoreCase=False):
        # 如果两个字符串完全相等，直接返回
        if p_str1 == p_str2:
            return True

        # 如果忽略大小写的情况下，两个字符串的大写相同，则直接返回
        if p_compare_ignoreCase:
            if p_str1.upper() == p_str2.upper():
                return True

        # 如果没有启用正则，则直接返回不相等
        if not p_compare_maskEnabled:
            return False

        # 用正则判断表达式是否相等
        try:
            # 对正则表达式进行预先编译
            if p_str2 in self.CompiledRegexPattern:
                m_CompiledPattern = self.CompiledRegexPattern[p_str2]
            else:
                # 如果之前编译过，且没有编译成功，不再尝试编译，直接判断不匹配
                if p_str2 in self.ErrorRegexPattern:
                    return False
                # 如果字符串内容不包含特定的正则字符，则直接失败，不再尝试编译
                # 如何快速判断？
                # 正则编译
                if p_compare_ignoreCase:
                    m_CompiledPattern = re.compile(p_str2, re.IGNORECASE)
                else:
                    m_CompiledPattern = re.compile(p_str2)
                self.CompiledRegexPattern[p_str2] = m_CompiledPattern
            matchObj = re.match(m_CompiledPattern, p_str1)
            if matchObj is None:
                return False
            elif str(matchObj.group()) != p_str1:
                return False
            else:
                return True
        except re.error:
            self.ErrorRegexPattern.append(p_str2)
            # 正则表达式错误，可能是由于这并非是一个正则表达式
            return False

    def compareMyers(self,
                     a_lines,
                     b_lines,
                     a_lineno,
                     b_lineno,
                     p_compare_maskEnabled=False,
                     p_compare_ignoreCase=False):
        # This marks the farthest-right point along each diagonal in the edit
        # graph, along with the history that got it there
        frontier = {1: self.Frontier(0, [])}

        history = []
        a_max = len(a_lines)
        b_max = len(b_lines)
        finished = False
        for d in range(0, a_max + b_max + 1):
            for k in range(-d, d + 1, 2):
                # This determines whether our next search point will be going down
                # in the edit graph, or to the right.
                #
                # The intuition for this is that we should go down if we're on the
                # left edge (k == -d) to make sure that the left edge is fully
                # explored.
                #
                # If we aren't on the top (k != d), then only go down if going down
                # would take us to territory that hasn't sufficiently been explored
                # yet.
                go_down = (k == -d or (k != d and frontier[k - 1].x < frontier[k + 1].x))

                # Figure out the starting point of this iteration. The diagonal
                # offsets come from the geometry of the edit grid - if you're going
                # down, your diagonal is lower, and if you're going right, your
                # diagonal is higher.
                if go_down:
                    old_x, history = frontier[k + 1]
                    x = old_x
                else:
                    old_x, history = frontier[k - 1]
                    x = old_x + 1

                # We want to avoid modifying the old history, since some other step
                # may decide to use it.
                history = history[:]
                y = x - k

                # We start at the invalid point (0, 0) - we should only start building
                # up history when we move off of it.
                if 1 <= y <= b_max and go_down:
                    history.append(self.Insert(b_lines[y - 1], b_lineno[y - 1]))
                elif 1 <= x <= a_max:
                    history.append(self.Remove(a_lines[x - 1], a_lineno[x - 1]))

                # Chew up as many diagonal moves as we can - these correspond to common lines,
                # and they're considered "free" by the algorithm because we want to maximize
                # the number of these in the output.
                while x < a_max and y < b_max and \
                        self.compare_string(a_lines[x], b_lines[y],
                                            p_compare_maskEnabled=p_compare_maskEnabled,
                                            p_compare_ignoreCase=p_compare_ignoreCase):
                    x += 1
                    y += 1
                    history.append(self.Keep(a_lines[x - 1], a_lineno[x - 1]))

                if x >= a_max and y >= b_max:
                    # If we're here, then we've traversed through the bottom-left corner,
                    # and are done.
                    finished = True
                    break
                else:
                    frontier[k] = self.Frontier(x, history)
            if finished:
                break
        compareResult = True
        compareDiffResult = []
        for elem in history:
            if isinstance(elem, self.Keep):
                compareDiffResult.append(" {:>{}} ".format(elem.lineno, 6) + elem.line)
            elif isinstance(elem, self.Insert):
                compareResult = False
                compareDiffResult.append("+{:>{}} ".format(elem.lineno, 6) + elem.line)
            else:
                compareResult = False
                compareDiffResult.append("-{:>{}} ".format(elem.lineno, 6) + elem.line)

        return compareResult, compareDiffResult

    def compareLCS(self,
                   x,
                   y,
                   linenox,
                   linenoy,
                   p_compare_maskEnabled=False,
                   p_compare_ignoreCase=False):
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
                                       p_compare_maskEnabled, p_compare_ignoreCase):
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
                                     p_compare_maskEnabled, p_compare_ignoreCase):
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
                           skipLines=None,
                           maskLines=None,
                           ignoreEmptyLine=False,
                           CompareWithMask=None,
                           CompareIgnoreCase=False,
                           CompareIgnoreTailOrHeadBlank=False,
                           CompareWorkEncoding='UTF-8',
                           CompareRefEncoding='UTF-8',
                           compareAlgorithm='LCS'):
        if not os.path.isfile(file1):
            raise DiffException('ERROR: %s is not a file' % file1)
        if not os.path.isfile(file2):
            raise DiffException('ERROR: %s is not a file' % file2)

        # 将比较文件加载到数组
        fileRawContent = open(file1, mode='r', encoding=CompareWorkEncoding).readlines()
        refFileRawContent = open(file2, mode='r', encoding=CompareRefEncoding).readlines()

        workFileContent = open(file1, mode='r', encoding=CompareWorkEncoding).readlines()
        refFileContent = open(file2, mode='r', encoding=CompareRefEncoding).readlines()

        # linno用来记录行号，在最后输出打印的时候，显示的是原始文件信息，而不是修正后的信息
        lineno1 = []
        lineno2 = []
        for m_nPos in range(0, len(workFileContent)):
            lineno1.append(m_nPos + 1)
        for m_nPos in range(0, len(refFileContent)):
            lineno2.append(m_nPos + 1)

        # 去掉filecontent中的回车换行
        for m_nPos in range(0, len(workFileContent)):
            if workFileContent[m_nPos].endswith('\n'):
                workFileContent[m_nPos] = workFileContent[m_nPos][:-1]
        for m_nPos in range(0, len(refFileContent)):
            if refFileContent[m_nPos].endswith('\n'):
                refFileContent[m_nPos] = refFileContent[m_nPos][:-1]

        # 去掉fileconent中的首尾空格
        if CompareIgnoreTailOrHeadBlank:
            for m_nPos in range(0, len(workFileContent)):
                workFileContent[m_nPos] = workFileContent[m_nPos].lstrip().rstrip()
            for m_nPos in range(0, len(refFileContent)):
                refFileContent[m_nPos] = refFileContent[m_nPos].lstrip().rstrip()

        # 去除在SkipLine里头的所有内容
        if skipLines is not None:
            m_nPos = 0
            while m_nPos < len(workFileContent):
                bMatch = False
                for pattern in skipLines:
                    if self.compare_string(workFileContent[m_nPos], pattern,
                                           p_compare_maskEnabled=True,
                                           p_compare_ignoreCase=True):
                        workFileContent.pop(m_nPos)
                        lineno1.pop(m_nPos)
                        bMatch = True
                        break
                if not bMatch:
                    m_nPos = m_nPos + 1

            m_nPos = 0
            while m_nPos < len(refFileContent):
                bMatch = False
                for pattern in skipLines:
                    if self.compare_string(refFileContent[m_nPos], pattern,
                                           p_compare_maskEnabled=True,
                                           p_compare_ignoreCase=True):
                        refFileContent.pop(m_nPos)
                        lineno2.pop(m_nPos)
                        bMatch = True
                        break
                if not bMatch:
                    m_nPos = m_nPos + 1

        # 去除所有的空行
        if ignoreEmptyLine:
            m_nPos = 0
            while m_nPos < len(workFileContent):
                if len(workFileContent[m_nPos].strip()) == 0:
                    workFileContent.pop(m_nPos)
                    lineno1.pop(m_nPos)
                else:
                    m_nPos = m_nPos + 1
            m_nPos = 0
            while m_nPos < len(refFileContent):
                if len(refFileContent[m_nPos].strip()) == 0:
                    refFileContent.pop(m_nPos)
                    lineno2.pop(m_nPos)
                else:
                    m_nPos = m_nPos + 1

        # 处理MaskLine中的信息，对ref, Work文件进行替换
        if maskLines is not None:
            m_nPos = 0
            while m_nPos < len(refFileContent):
                for pattern in maskLines:
                    m_SQLMask = pattern.split("=>")
                    if len(m_SQLMask) == 2:
                        m_SQLMaskPattern = m_SQLMask[0]
                        m_SQLMaskTarget = m_SQLMask[1]
                        if re.search(m_SQLMaskPattern, refFileContent[m_nPos], re.IGNORECASE) is not None:
                            refFileContent[m_nPos] = \
                                re.sub(m_SQLMaskPattern, m_SQLMaskTarget, refFileContent[m_nPos], flags=re.IGNORECASE)
                    else:
                        print("LogMask Hint Error, missed =>: [" + pattern + "]")
                m_nPos = m_nPos + 1
            m_nPos = 0
            while m_nPos < len(workFileContent):
                for pattern in maskLines:
                    m_SQLMask = pattern.split("=>")
                    if len(m_SQLMask) == 2:
                        m_SQLMaskPattern = m_SQLMask[0]
                        m_SQLMaskTarget = m_SQLMask[1]
                        if re.search(m_SQLMaskPattern, workFileContent[m_nPos], re.IGNORECASE) is not None:
                            workFileContent[m_nPos] = \
                                re.sub(m_SQLMaskPattern, m_SQLMaskTarget, workFileContent[m_nPos], flags=re.IGNORECASE)
                    else:
                        print("LogMask Hint Error, missed =>: [" + pattern + "]")
                m_nPos = m_nPos + 1

        # 输出两个信息
        # 1：  Compare的结果是否存在dif，True/False
        # 2:   Compare的Dif列表. 注意：LCS算法是一个翻转的列表. MYERS算法里头是一个正序列表
        if compareAlgorithm == "MYERS":
            (m_CompareResult, m_CompareResultList) = self.compareMyers(workFileContent, refFileContent, lineno1,
                                                                       lineno2,
                                                                       p_compare_maskEnabled=CompareWithMask,
                                                                       p_compare_ignoreCase=CompareIgnoreCase)
        else:
            (m_CompareResult, m_CompareResultList) = self.compareLCS(workFileContent, refFileContent, lineno1, lineno2,
                                                                     p_compare_maskEnabled=CompareWithMask,
                                                                     p_compare_ignoreCase=CompareIgnoreCase)
            # 翻转数组
            m_CompareResultList = m_CompareResultList[::-1]
        # 随后从数组中补充进入被Skip掉的内容
        m_nWorkLastPos = 0  # 上次Work文件已经遍历到的位置
        m_nRefLastPos = 0  # 上次Ref文件已经遍历到的位置
        m_NewCompareResultList = []
        # 从列表中反向开始遍历， Step=-1
        for row in m_CompareResultList:
            if row.startswith('+'):
                # 当前日志没有，Reference Log中有的
                # 需要注意的是，Ref文件中被跳过的行不会补充进入dif文件
                m_LineNo = int(row[1:7])
                m_AppendLine = "+{:>{}} ".format(m_LineNo, 6) + refFileRawContent[m_LineNo - 1]
                if m_AppendLine.endswith("\n"):
                    m_AppendLine = m_AppendLine[:-1]
                m_NewCompareResultList.append(m_AppendLine)
                m_nRefLastPos = m_LineNo
                continue
            elif row.startswith('-'):
                # 当前日志有，但是Reference里头没有的
                m_LineNo = int(row[1:7])
                # 补充填写那些已经被忽略规则略掉的内容，只填写LOG文件中的对应信息
                if m_LineNo > (m_nWorkLastPos + 1):
                    # 当前日志中存在，但是比较的过程中被Skip掉的内容，要首先补充进来
                    for m_nPos in range(m_nWorkLastPos + 1, m_LineNo):
                        m_AppendLine = "S{:>{}} ".format(m_nPos, 6) + fileRawContent[m_nPos - 1]
                        if m_AppendLine.endswith("\n"):
                            m_AppendLine = m_AppendLine[:-1]
                        m_NewCompareResultList.append(m_AppendLine)
                m_AppendLine = "-{:>{}} ".format(m_LineNo, 6) + fileRawContent[m_LineNo - 1]
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
                    for m_nPos in range(m_nWorkLastPos + 1, m_LineNo):
                        m_AppendLine = "S{:>{}} ".format(m_nPos, 6) + fileRawContent[m_nPos - 1]
                        if m_AppendLine.endswith("\n"):
                            m_AppendLine = m_AppendLine[:-1]
                        m_NewCompareResultList.append(m_AppendLine)
                # 完全一样的内容
                m_AppendLine = " {:>{}} ".format(m_LineNo, 6) + fileRawContent[m_LineNo - 1]
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
    c_compareAlgorithm = "LCS"                # Diff算法

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
        if p_szParameter.upper() == "compareAlgorithm".upper():
            if p_szValue.upper() == "MYERS":
                self.c_compareAlgorithm = 'MYERS'
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
                skipLines=self.c_SkipLines,
                maskLines=self.c_MaskLines,
                ignoreEmptyLine=self.c_IgnoreEmptyLine,
                CompareWithMask=self.c_CompareEnableMask,
                CompareIgnoreCase=self.c_CompareIgnoreCase,
                CompareIgnoreTailOrHeadBlank=self.c_CompareIgnoreTailOrHeadBlank,
                CompareWorkEncoding=self.c_CompareWorkEncoding,
                CompareRefEncoding=self.c_CompareRefEncoding,
                compareAlgorithm=self.c_compareAlgorithm
            )

            # 生成Scenario分析结果
            m_ScenarioStartPos = 0  # 当前Scenario开始的位置
            m_ScenarioResults = {}
            m_ScenariosPos = {}

            # 首先记录下来每一个Scenario的开始位置，结束位置
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
                    m_ScenarioAndPriority = match_obj.group(3).strip()
                    if len(m_ScenarioAndPriority.split(':')) == 2:
                        # 重复的Scenario开始
                        if m_ScenarioName is not None and \
                                m_ScenarioAndPriority.split(':')[1].strip() == m_ScenarioName:
                            m_ScenarioStartPos = pos
                            pos = pos + 1
                            continue
                    else:
                        # 重复的Scenario开始
                        if m_ScenarioName is not None and \
                                m_ScenarioName == m_ScenarioAndPriority:
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
                    if len(m_ScenarioAndPriority.split(':')) == 2:
                        # 如果有两个内容， 规则是:Scenario:Priority:ScenarioName
                        m_ScenarioPriority = m_ScenarioAndPriority.split(':')[0].strip()
                        m_ScenarioName = m_ScenarioAndPriority.split(':')[1].strip()
                    else:
                        # 如果只有一个内容， 规则是:Scenario:ScenarioName
                        m_ScenarioName = m_ScenarioAndPriority
                        m_ScenarioPriority = None
                    m_ScenarioStartPos = pos
                    pos = pos + 1
                    continue

                match_obj = re.search(r"--(\s+)?\[(\s+)?Scenario:(.*)]", m_CompareResultList[pos],
                                      re.IGNORECASE | re.DOTALL)
                if match_obj:
                    m_ScenarioAndPriority = match_obj.group(3).strip()
                    if len(m_ScenarioAndPriority.split(':')) == 2:
                        # 重复的Scenario开始
                        if m_ScenarioName is not None and \
                                m_ScenarioAndPriority.split(':')[1].strip() == m_ScenarioName:
                            m_ScenarioStartPos = pos
                            pos = pos + 1
                            continue
                    else:
                        # 重复的Scenario开始
                        if m_ScenarioName is not None and \
                                m_ScenarioName == m_ScenarioAndPriority:
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
                    if len(m_ScenarioAndPriority.split(':')) == 2:
                        # 如果有两个内容， 规则是:Scenario:Priority:ScenarioName
                        m_ScenarioPriority = m_ScenarioAndPriority.split(':')[0].strip()
                        m_ScenarioName = m_ScenarioAndPriority.split(':')[1].strip()
                    else:
                        # 如果只有一个内容， 规则是:Scenario:ScenarioName
                        m_ScenarioName = m_ScenarioAndPriority
                        m_ScenarioPriority = None
                    m_ScenarioStartPos = pos
                    pos = pos + 1
                    continue

                # 不是什么特殊内容，这里是标准文本
                pos = pos + 1

            # 最后一个Scenario的情况记录下来
            if m_ScenarioStartPos < len(m_CompareResultList):
                if m_ScenarioName is not None:
                    m_ScenariosPos[m_ScenarioName] = {
                        "ScenarioStartPos": m_ScenarioStartPos,
                        "ScenarioEndPos": len(m_CompareResultList),
                        "ScenarioPriority": m_ScenarioPriority
                    }
            # 遍历每一个Scenario的情况
            for m_ScenarioName, m_Scenario_Pos in m_ScenariosPos.items():
                start_pos = m_Scenario_Pos['ScenarioStartPos']
                m_EndPos = m_Scenario_Pos['ScenarioEndPos']
                m_ScenarioPriority = m_Scenario_Pos['ScenarioPriority']
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

        match_obj = re.match(r"test\s+set\s+(.*?)\s+(.*?)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_Parameter = match_obj.group(1).strip()
            m_Value = match_obj.group(2).strip()
            self.setTestOptions(m_Parameter, m_Value)
            return None, None, None, None, "set successful."

        match_obj = re.match(r"test\s+compare\s+(.*)\s+with\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_WorkFile = match_obj.group(1).strip()
            m_RefFile = match_obj.group(2).strip()
            if m_WorkFile.startswith("'") and m_WorkFile.endswith("'"):
                m_WorkFile = m_WorkFile[1:-1]
            if m_RefFile.startswith("'") and m_RefFile.endswith("'"):
                m_RefFile = m_RefFile[1:-1]
            (title, result, headers, columnTypes, status) = self.Compare_Files(m_WorkFile, m_RefFile)
            return title, result, headers, columnTypes, status

        match_obj = re.match(r"test\s+assert\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_formular = match_obj.group(1).strip()
            (title, result, headers, columnTypes, status) = self.AssertFormular(m_formular)
            return title, result, headers, columnTypes, status

        match_obj = re.match(r"test\s+loadenv\s+(.*)\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_EnvFileName = match_obj.group(1).strip()
            m_EnvSectionName = match_obj.group(2).strip()
            (title, result, headers, columnTypes, status) = self.LoadEnv(m_EnvFileName, m_EnvSectionName)
            return title, result, headers, columnTypes, status

        return None, None, None, None, "Unknown test Command."
