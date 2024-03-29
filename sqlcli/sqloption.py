# -*- coding: utf-8 -*-
"""处理程序运行的各种参数."""
import os


class SQLOptions(object):
    """处理程序运行的各种参数."""
    def __init__(self):
        self.m_SQL_OptionList = []

        self.m_SQL_OptionList.append({"Name": "WHENEVER_SQLERROR",
                                      "Value": "CONTINUE",
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "PAGE",
                                      "Value": "OFF",
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "ECHO",
                                      "Value": "ON",
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "TIMING",
                                      "Value": "OFF",
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "TIME",
                                      "Value": "OFF",
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "CSV_HEADER",
                                      "Value": "OFF",
                                      "Comments": 'ON|OFF',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "CSV_DELIMITER",
                                      "Value": ",",
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "CSV_QUOTECHAR",
                                      "Value": "",
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "FEEDBACK",
                                      "Value": "ON",
                                      "Comments": 'ON|OFF',
                                      "Hidden": False
                                      })
        self.m_SQL_OptionList.append({"Name": "TERMOUT",
                                      "Value": "ON",
                                      "Comments": 'ON|OFF',
                                      "Hidden": False
                                      })
        self.m_SQL_OptionList.append({"Name": "ARRAYSIZE",
                                      "Value": 10000,
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "SQLREWRITE",
                                      "Value": "ON",
                                      "Comments": 'ON|OFF',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "LOB_LENGTH",
                                      "Value": 20,
                                      "Comments": '',
                                      "Hidden": False})

        self.m_SQL_OptionList.append({"Name": "FLOAT_FORMAT",
                                      "Value": "%.7g",
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "DECIMAL_FORMAT",
                                      "Value": "",
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "DATE_FORMAT",
                                      "Value": "%Y-%m-%d",
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "DATETIME_FORMAT",
                                      "Value": "%Y-%m-%d %H:%M:%S.%f",
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "TIME_FORMAT",
                                      "Value": "%H:%M:%S.%f",
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "DATETIME-TZ_FORMAT",
                                      "Value": "%Y-%m-%d %H:%M:%S %z",
                                      "Comments": '',
                                      "Hidden": False})

        self.m_SQL_OptionList.append({"Name": "OUTPUT_SORT_ARRAY",
                                      "Value": "ON",
                                      "Comments": 'Print Array output with sort order.',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "OUTPUT_PREFIX",
                                      "Value": "",
                                      "Comments": 'Output Prefix',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "OUTPUT_ERROR_PREFIX",
                                      "Value": "",
                                      "Comments": 'Error Output Prefix',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "OUTPUT_FORMAT",
                                      "Value": "LEGACY",
                                      "Comments": 'TAB|CSV|LEGACY',
                                      "Hidden": False
                                      })

        self.m_SQL_OptionList.append({"Name": "CONN_RETRY_TIMES",
                                      "Value": "1",
                                      "Comments": 'Connect retry times.',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "DEBUG",
                                      "Value": "OFF",
                                      "Comments": 'ON|OFF',
                                      "Hidden": True})
        self.m_SQL_OptionList.append({"Name": "CONNURL",
                                      "Value": "",
                                      "Comments": 'Connection URL',
                                      "Hidden": True})
        self.m_SQL_OptionList.append({"Name": "CONNPROP",
                                      "Value": "",
                                      "Comments": 'Connection Props',
                                      "Hidden": True})
        self.m_SQL_OptionList.append({"Name": "SILENT",
                                      "Value": "OFF",
                                      "Comments": '',
                                      "Hidden": True})
        self.m_SQL_OptionList.append({"Name": "SQL_EXECUTE",
                                      "Value": "PREPARE",
                                      "Comments": 'DIRECT|PREPARE',
                                      "Hidden": False})

        self.m_SQL_OptionList.append({"Name": "JOBMANAGER",
                                      "Value": "OFF",
                                      "Comments": 'ON|OFF',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "JOBMANAGER_METAURL",
                                      "Value": "",
                                      "Comments": '',
                                      "Hidden": False})

        self.m_SQL_OptionList.append({"Name": "SCRIPT_TIMEOUT",
                                      "Value": -1,
                                      "Comments": '',
                                      "Hidden": False})
        self.m_SQL_OptionList.append({"Name": "SQL_TIMEOUT",
                                      "Value": -1,
                                      "Comments": '',
                                      "Hidden": False})

        self.m_SQL_OptionList.append({"Name": "PRIORITY",
                                      "Value": "",
                                      "Comments": '',
                                      "Hidden": False})

        self.m_SQL_OptionList.append({"Name": "SCRIPT_ENCODING",
                                      "Value": "UTF-8",
                                      "Comments": '',
                                      "Hidden": False})

        self.m_SQL_OptionList.append({"Name": "RESULT_ENCODING",
                                      "Value": "UTF-8",
                                      "Comments": '',
                                      "Hidden": False})

    def get(self, p_ParameterName):
        """根据参数名称返回参数，若不存在该参数，返回None."""
        for item in self.m_SQL_OptionList:
            if item["Name"] == p_ParameterName:
                return item["Value"]
        return None

    def getOptionList(self):
        """返回全部的运行参数列表"""
        return self.m_SQL_OptionList

    def set(self, p_ParameterName, p_ParameterValue, p_ParameterDefaultValue=None, p_Hidden=False):
        """设置运行参数， 若p_ParameterValue为空，则加载默认参数"""
        for pos in range(0, len(self.m_SQL_OptionList)):
            if self.m_SQL_OptionList[pos]["Name"] == p_ParameterName:
                if p_ParameterValue is None:
                    m_ParameterValue = None
                else:
                    m_ParameterValue = str(p_ParameterValue).strip()
                    if m_ParameterValue.upper().startswith("${ENV(") and m_ParameterValue.upper().endswith(")}"):
                        m_EnvName = m_ParameterValue[6:-2]
                        if m_EnvName in os.environ:
                            m_ParameterValue = os.environ[m_EnvName]
                        else:
                            m_ParameterValue = None
                if m_ParameterValue is None:
                    if p_ParameterDefaultValue is None:
                        m_ParameterValue = ""
                    else:
                        m_ParameterValue = p_ParameterDefaultValue
                self.m_SQL_OptionList[pos]["Value"] = m_ParameterValue
                return True

        # 对@开头的进行保存和处理
        m_ParameterName = p_ParameterName.strip()
        if m_ParameterName.startswith("@"):
            m_ParameterValue = p_ParameterValue.strip()
            if m_ParameterValue.upper().startswith("${ENV(") and m_ParameterValue.upper().endswith(")}"):
                m_EnvName = m_ParameterValue[6:-2]
                if m_EnvName in os.environ:
                    m_ParameterValue = os.environ[m_EnvName]
                else:
                    m_ParameterValue = None

            if m_ParameterValue is None:
                if p_ParameterDefaultValue is None:
                    m_ParameterValue = ""
                else:
                    m_ParameterValue = p_ParameterDefaultValue
            self.m_SQL_OptionList.append({"Name": m_ParameterName,
                                          "Value": m_ParameterValue,
                                          "Hidden": p_Hidden,
                                          "Comments": 'User session variable'})
            return True

        # 对于不认识的参数信息，直接抛出到上一级别，做CommmonNotFound处理
        return False
