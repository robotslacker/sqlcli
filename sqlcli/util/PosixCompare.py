# -*- coding: utf-8 -*-

import argparse
import os
import re


class DiffException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class POSIXCompare:
    compare_options = argparse.Namespace()
    compare_result = True

    def __init__(self):
        self.compare_options.isMaskEnabled = False

    def lcs_len(self, x, y):
        """Build a matrix of LCS length.
        This matrix will be used later to backtrack the real LCS.
        """

        # This is our matrix comprised of list of lists.
        # We allocate extra row and column with zeroes for the base case of empty
        # sequence. Extra row and column is appended to the end and exploit
        # Python's ability of negative indices: x[-1] is the last elem.
        c = [[0 for _ in range(len(y) + 1)] for _ in range(len(x) + 1)]

        for i, xi in enumerate(x):
            for j, yj in enumerate(y):
                if self.compare_string(xi, yj):
                    c[i][j] = 1 + c[i - 1][j - 1]
                else:
                    c[i][j] = max(c[i][j - 1], c[i - 1][j])
        return c

    def backtrack(self, c, x, y, i, j):
        """Backtrack the LCS length matrix to get the actual LCS"""
        if i == -1 or j == -1:
            return ""
        elif self.compare_string(x[i], y[j]):
            return self.backtrack(c, x, y, i - 1, j - 1) + x[i]
        elif c[i][j - 1] >= c[i - 1][j]:
            return self.backtrack(c, x, y, i, j - 1)
        elif c[i][j - 1] < c[i - 1][j]:
            return self.backtrack(c, x, y, i - 1, j)

    def lcs(self, x, y):
        """Get the longest common subsequence of x and y"""
        c = self.lcs_len(x, y)
        return self.backtrack(c, x, y, len(x) - 1, len(y) - 1)

    def compare_string(self, p_str1, p_str2):
        if not self.compare_options.isMaskEnabled:
            return p_str1 == p_str2
        if self.compare_options.isMaskEnabled:
            return re.match(p_str2, p_str1) is not None

    def compare(self, c, x, y, i, j, p_result):
        """Print the diff using LCS length matrix by backtracking it"""

        if i < 0 and j < 0:
            return p_result
        elif i < 0:
            self.compare(c, x, y, i, j - 1, p_result)
            self.compare_result = False
            p_result.append("+ " + y[j])
        elif j < 0:
            self.compare(c, x, y, i - 1, j, p_result)
            self.compare_result = False
            p_result.append("- " + x[i])
        elif self.compare_string(x[i], y[j]):
            self.compare(c, x, y, i - 1, j - 1, p_result)
            p_result.append("  " + x[i])
        elif c[i][j - 1] >= c[i - 1][j]:
            self.compare(c, x, y, i, j - 1, p_result)
            self.compare_result = False
            p_result.append("+ " + y[j])
        elif c[i][j - 1] < c[i - 1][j]:
            self.compare(c, x, y, i - 1, j, p_result)
            self.compare_result = False
            p_result.append("- " + x[i])
        return self.compare_result, p_result

    def set_compare_options(self, options):
        self.compare_options = options

    def compare_text_files(self, file1, file2):
        if not os.path.isfile(file1):
            raise DiffException('ERROR: %s is not a file' % file1)
        if not os.path.isfile(file2):
            raise DiffException('ERROR: %s is not a file' % file2)

        file1content = open(file1, mode='r').readlines()
        file2content = open(file2, mode='r').readlines()

        m_lcs = self.lcs_len(file1content, file2content)
        m_result = []
        return self.compare(m_lcs, file1content, file2content,
                            len(file1content) - 1, len(file2content) - 1, m_result)
