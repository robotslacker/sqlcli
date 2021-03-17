# -*- coding: UTF-8 -*-
import re
import os
import time
import datetime
import traceback
from .sqlcliexception import SQLCliException
import concurrent.futures

from .datawrapper import DataWrapper
try:
    from confluent_kafka import Producer, Consumer, TopicPartition, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
except ImportError:
    # Windows 目前安装confluent_kafka 存在问题，计划废弃
    pass


class KafkaWrapperException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class KafkaWrapper(object):

    def __init__(self):
        self.__kafka_servers__ = None
        self.__kafka_createtopic_timeout = 60
        self.__kafka_deletetopic_timeout = 1800
        self.m_DataWrapper = DataWrapper()

    def Kafka_Connect(self, p_szKafkaServers):
        self.__kafka_servers__ = p_szKafkaServers

    def Kafka_CreateTopic(self, p_szTopicName,
                          p_Partitons, p_replication_factor,
                          p_ConfigProps,
                          p_TimeOut=0):
        if self.__kafka_servers__ is None:
            raise SQLCliException("Missed kafka server information. Please use \"kafka set server\" first ..")
        a = AdminClient({'bootstrap.servers': self.__kafka_servers__})
        new_topics = [NewTopic(p_szTopicName,
                               num_partitions=p_Partitons,
                               replication_factor=p_replication_factor,
                               config=p_ConfigProps
                               ), ]
        fs = a.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                bCreateSuccessFul = False
                # 循环判断TOPIC是否已经创建成功
                # 默认的情况下，Kafka不等创建完全完成，就会返回，这可能导致随后的发送消息失败
                if p_TimeOut == 0:
                    m_TimeOut = self.__kafka_createtopic_timeout
                else:
                    m_TimeOut = p_TimeOut
                for nPos in range(0, m_TimeOut):
                    if len(a.list_topics(topic=p_szTopicName).topics[p_szTopicName].partitions.keys()) != 0:
                        bCreateSuccessFul = True
                        break
                    else:
                        time.sleep(1)
                        continue
                if bCreateSuccessFul:
                    return "Topic {} created.".format(topic)
                else:
                    return "Timeout while wait for topic {} creation.".format(topic)
            except KafkaException as ke:
                if "SQLCLI_DEBUG" in os.environ:
                    print('traceback.print_exc():\n%s' % traceback.print_exc())
                    print('traceback.format_exc():\n%s' % traceback.format_exc())
                return "Failed to create topic {}: {}".format(topic, repr(ke))

    def Kafka_DeleteTopic(self, p_szTopicName, p_TimeOut: int):
        if self.__kafka_servers__ is None:
            raise SQLCliException("Missed kafka server information. Please use \"kafka set server\" first ..")
        a = AdminClient({'bootstrap.servers': self.__kafka_servers__})
        deleted_topics = [p_szTopicName, ]
        fs = a.delete_topics(topics=deleted_topics)
        for topic, f in fs.items():
            try:
                f.result()
                # 循环判断TOPIC是否已经删除完毕
                # 默认的情况下，Kafka不等删除完全完成，就会返回，这可能导致随后的其他操作失败
                bDeleteSuccessFul = False
                if p_TimeOut == 0:
                    m_TimeOut = self.__kafka_deletetopic_timeout
                else:
                    m_TimeOut = p_TimeOut
                for nPos in range(0, m_TimeOut):
                    bTopicExists = False
                    fs2 = a.create_topics(
                        new_topics=[NewTopic(p_szTopicName, num_partitions=1, replication_factor=1), ],
                        validate_only=True)
                    for topic2, f2 in fs2.items():
                        try:
                            f2.result()
                        except KafkaException as ke:
                            if ke.args[0].name() == "TOPIC_ALREADY_EXISTS":
                                bTopicExists = True
                                time.sleep(1)
                                continue
                    if not bTopicExists:
                        bDeleteSuccessFul = True
                        break
                if bDeleteSuccessFul:
                    return "Topic {} deleted.".format(topic)
                else:
                    return "Timeout while wait for topic {} deletion.".format(topic)
            except KafkaException as ke:
                if repr(ke).find("UNKNOWN_TOPIC_OR_PART") != -1:
                    return "Topic {} deleted.".format(topic)
                else:
                    if "SQLCLI_DEBUG" in os.environ:
                        print('traceback.print_exc():\n%s' % traceback.print_exc())
                        print('traceback.format_exc():\n%s' % traceback.format_exc())
                return "Failed to drop topic {}: {}".format(topic, repr(ke))

    def kafka_GetInfo(self, p_szTopicName):
        if self.__kafka_servers__ is None:
            raise SQLCliException("Missed kafka server information. Please use \"kafka set server\" first ..")
        a = AdminClient({'bootstrap.servers': self.__kafka_servers__})
        configs = a.describe_configs(resources=[ConfigResource(ConfigResource.Type["TOPIC"], p_szTopicName)])
        nReturnMessages = None
        for f in concurrent.futures.as_completed(iter(configs.values())):
            if nReturnMessages is None:
                nReturnMessages = "ConfigProperties: \n" + \
                                  "  " + str(f.result(timeout=1)["retention.ms"])
            else:
                nReturnMessages = nReturnMessages + "\n" + \
                                  "ConfigProperties: \n" + \
                                  "  " + str(f.result(timeout=1)["retention.ms"])
        return nReturnMessages

    def kafka_GetOffset(self, p_szTopicName, p_szGroupID=''):
        if self.__kafka_servers__ is None:
            raise SQLCliException("Missed kafka server information. Please use set kafka server first ..")
        c = Consumer({'bootstrap.servers': self.__kafka_servers__, 'group.id': p_szGroupID, })
        m_OffsetResults = []
        try:
            for pid in c.list_topics(topic=p_szTopicName).topics[p_szTopicName].partitions.keys():
                tp = TopicPartition(p_szTopicName, pid)
                (low, high) = c.get_watermark_offsets(tp)
                m_OffsetResults.append([pid, low, high])
            if len(m_OffsetResults) == 0:
                raise SQLCliException("Topic [" + p_szTopicName + "] does not exist!")
            return m_OffsetResults
        except KafkaException as ke:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
            raise ke

    def kafka_Produce(self, p_szTopicName, p_Message, p_ErrorList):
        def delivery_report(err, msg):
            if err is not None:
                p_ErrorList.append({"err": err, "msg": msg})

        try:
            nCount = 0
            p_ErrorList = []
            p = Producer({'bootstrap.servers': self.__kafka_servers__, })
            p.poll(0)
            if isinstance(p_Message, str):
                nCount = nCount + 1
                p.produce(p_szTopicName, p_Message.encode('utf-8'), callback=delivery_report)
            elif isinstance(p_Message, list):
                for m_Message in p_Message:
                    p.produce(p_szTopicName, m_Message.encode('utf-8'), callback=delivery_report)
                    nCount = nCount + 1
                    if (nCount % 5000) == 0:
                        p.flush()
                    if len(p_ErrorList) > 1000:
                        p.flush()
                        raise KafkaWrapperException("Too much error exception ..., produce aborted.")
            p.flush()
            return nCount
        except KafkaException as ke:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
            raise ke

    def kafka_Consume(self, p_szTopicName, p_szGroupID="", p_nBatchSize=1000, p_nTimeOut=10):
        c = Consumer({'bootstrap.servers': self.__kafka_servers__,
                      'group.id': p_szGroupID,
                      'default.topic.config': {'auto.offset.reset': 'earliest'}}
                     )
        c.subscribe([p_szTopicName, ])
        message_return = []
        nCount = 0
        while True:
            msg = c.poll(timeout=p_nTimeOut)
            if msg is None:
                # 已经没有消息了
                break
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                break
            func = getattr(msg, 'value')
            message_return.append(func().decode('utf-8'))
            nCount = nCount + 1
            if nCount > p_nBatchSize:
                break
        c.close()
        yield message_return

    def Process_SQLCommand(self, p_szSQL):
        m_szSQL = p_szSQL.strip()

        matchObj = re.match(r"kafka\s+connect\s+server\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_KafkaServer = str(matchObj.group(1)).strip()
            self.__kafka_servers__ = m_KafkaServer
            return None, None, None, None, "Kafka Server set successful."

        matchObj = re.match(r"kafka\s+create\s+topic\s+(.*?)\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            m_ParameterList = str(matchObj.group(2)).strip().split()
            m_TimeOut = 0            # 默认的延时等待时间为: 一直等待下去
            m_PartitionCount = 16    # 默认的Kafka分区数量
            m_ReplicationFactor = 1  # 默认的副本数量
            m_ConfigProps = {}
            for m_nPos in range(0, len(m_ParameterList) // 2):
                m_ParameterName = m_ParameterList[2*m_nPos]
                m_ParameterValue = m_ParameterList[2 * m_nPos + 1]
                if m_ParameterName.lower() == "partitions":
                    m_PartitionCount = int(m_ParameterValue)
                elif m_ParameterName.lower() == "replication_factor":
                    m_ReplicationFactor = int(m_ParameterValue)
                elif m_ParameterName.lower() == "timeout":
                    m_TimeOut = int(m_ParameterValue)
                else:
                    m_ConfigProps[m_ParameterName] = m_ParameterValue
            m_ReturnMessage = self.Kafka_CreateTopic(
                m_TopicName, m_PartitionCount, m_ReplicationFactor,
                m_ConfigProps,
                m_TimeOut)
            return None, None, None, None, m_ReturnMessage

        matchObj = re.match(r"kafka\s+drop\s+topic\s+(.*)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            matchObj = re.match(r"kafka\s+drop\s+topic\s+(.*)\s+timeout\s+(\d+)(\s+)?$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_TimeOut = int(matchObj.group(2))
            else:
                m_TimeOut = 1800
            m_ReturnMessage = self.Kafka_DeleteTopic(p_szTopicName=m_TopicName, p_TimeOut=m_TimeOut)
            return None, None, None, None, m_ReturnMessage

        # 显示所有Partition的偏移数据
        matchObj = re.match(r"get\s+kafka\s+info\s+topic(.*)\s+group\s+(.*)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            m_GroupID = str(matchObj.group(2)).strip()
            try:
                m_Results = self.kafka_GetOffset(m_TopicName, m_GroupID)
                m_Header = ["Partition", "minOffset", "maxOffset"]
                m_nTotalOffset = 0
                for m_Result in m_Results:
                    m_nTotalOffset = m_nTotalOffset + int(m_Result[2]) - int(m_Result[1])
                m_Message = "Total " + str(len(m_Results)) + " partitions, total offset is " + str(m_nTotalOffset) + "."
                return None, m_Results, m_Header, None, m_Message
            except KafkaException as ke:
                return None, None, None, None, "Failed to get office for topic {}: {}".format(m_TopicName, repr(ke))

        # 显示所有Partition的偏移数据
        matchObj = re.match(r"kafka\s+get\s+info\s+topic(.*)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            m_GroupID = 'asdfzxcv1234'    # 一个随机的字符串
            try:
                m_MetadataInfo = self.kafka_GetInfo(m_TopicName)
                m_Results = self.kafka_GetOffset(m_TopicName, m_GroupID)
                m_Header = ["Partition", "minOffset", "maxOffset"]
                m_nTotalOffset = 0
                for m_Result in m_Results:
                    m_nTotalOffset = m_nTotalOffset + int(m_Result[2]) - int(m_Result[1])
                m_Message = "Total " + str(len(m_Results)) + " partitions, total offset is " + str(m_nTotalOffset) + "."
                return m_MetadataInfo, m_Results, m_Header, None, m_Message
            except KafkaException as ke:
                return None, None, None, None, "Failed to get office for topic {}: {}".format(m_TopicName, repr(ke))

        # 从文件中加载消息到Kafka队列中
        matchObj = re.match(r"kafka\s+produce\s+message\s+from\s+file\s+(.*)\s+to\s+topic\s+(.*)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_FileName = str(matchObj.group(1)).strip()
            m_TopicName = str(matchObj.group(2)).strip()
            if not os.path.isfile(m_FileName):
                return None, None, None, None, "Failed to load file {}".format(m_FileName)
            with open(m_FileName, 'r', encoding="utf-8") as f:
                m_MessageList = []
                m_nRows = 0
                m_nMessagesSent = 0
                m_nMessagesError = 0
                for line in f:
                    if line[-1:] == '\n':
                        m_MessageList.append(line[:-1])
                    else:
                        m_MessageList.append(line)
                    m_nRows = m_nRows + 1
                    if m_nRows % 5000 == 0:
                        # 每5000条发送一次消息
                        m_ProduceError = []
                        try:
                            m_nMessagesSent = m_nMessagesSent + \
                                              self.kafka_Produce(m_TopicName, m_MessageList, m_ProduceError)
                            m_nMessagesError = m_nMessagesError + len(m_ProduceError)
                        except (KafkaException, KafkaWrapperException) as ke:
                            return None, None, None, None, \
                                   "Failed to send message for topic {}: {}".format(m_TopicName, repr(ke))
                        m_MessageList.clear()
                # 发送剩下的所有消息
                if len(m_MessageList) != 0:
                    m_ProduceError = []
                    try:
                        m_nMessagesSent = m_nMessagesSent + \
                                          self.kafka_Produce(m_TopicName, m_MessageList, m_ProduceError)
                        m_nMessagesError = m_nMessagesError + len(m_ProduceError)
                    except (KafkaException, KafkaWrapperException) as ke:
                        return None, None, None, None, \
                               "Failed to send message for topic {}: {}".format(m_TopicName, repr(ke))
                if m_nMessagesError != 0:
                    return None, None, None, None, "Total {}/{} messages send to topic {} with {} failed.". \
                        format(m_nMessagesSent, m_nRows, m_TopicName, m_nMessagesError)
                else:
                    return None, None, None, None, "Total {}/{} messages send to topic {} Successful". \
                        format(m_nMessagesSent, m_nRows, m_TopicName)

        # 整体一次性发送消息
        matchObj = re.match(r"kafka\s+produce\s+message\s+topic\s+(.*?)\((.*)\)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            m_RawMessages = str(matchObj.group(2)).split('\n')
            m_Messages = []
            for m_nPos in range(0, len(m_RawMessages)):
                if len(m_RawMessages[m_nPos]) != 0:
                    m_Messages.append(self.m_DataWrapper.get_final_string(
                        self.m_DataWrapper.parse_formula_str(m_RawMessages[m_nPos])))
            m_ProduceError = []
            try:
                nTotalCount = self.kafka_Produce(m_TopicName, m_Messages, m_ProduceError)
                if len(m_ProduceError) != 0:
                    return None, None, None, None, "Total {} messages send to topic {} with {} failed.".\
                        format(nTotalCount, m_TopicName, len(m_ProduceError))
                else:
                    return None, None, None, None, "Total {} messages send to topic {} Successful".\
                        format(nTotalCount, m_TopicName)
            except (KafkaException, KafkaWrapperException) as ke:
                return None, None, None, None, "Failed to send message for topic {}: {}".format(m_TopicName, repr(ke))

        # 逐条发送消息
        matchObj = re.match(r"kafka\s+produce\s+message\s+topic\s+(.*?)"
                            r"\((.*)\)(\s+)?rows\s+(\d+)"
                            r"(.*?)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            m_formula_str = str(matchObj.group(2)).replace('\r', '').replace('\n', '').strip()
            m_row_struct = self.m_DataWrapper.parse_formula_str(m_formula_str)
            m_row_count = int(str(matchObj.group(4)).strip())
            m_ErrorCount = 0
            nTotalCount = 0
            m_frequency = -1             # -1表示不显示发送频率
            m_BatchSize = 5000          # 每一个批次发送的数据量
            matchObj = re.match(r"kafka\s+produce\s+message\s+topic\s+(.*?)"
                                r"\((.*)\)(\s+)?rows\s+(\d+)"
                                r"\s+frequency\s+(\d+)"
                                r"(\s+)?$",
                                m_szSQL, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_row_count = int(str(matchObj.group(4)).strip())
                m_frequency = int(str(matchObj.group(5)).strip())
                if m_frequency < 5000:
                    m_BatchSize = m_frequency  # 如果限制了速率，则最多每一个批次发送freqency数量的记录
            try:
                starttime = int(time.mktime(datetime.datetime.now().timetuple()))
                # 循环按照BatchSize发送消息
                for i in range(0, m_row_count // m_BatchSize):
                    m_Messages = []
                    for j in range(0, m_BatchSize):
                        m_Messages.append(self.m_DataWrapper.get_final_string(m_row_struct))
                    m_ProduceError = []
                    nTotalCount = nTotalCount + self.kafka_Produce(m_TopicName, m_Messages, m_ProduceError)
                    if m_frequency != -1:
                        # 判断当前时间，若发送次数超过了频率要求，则休息一段时间
                        currenttime = int(time.mktime(datetime.datetime.now().timetuple()))
                        if (currenttime - starttime) * m_frequency < nTotalCount:
                            time.sleep((nTotalCount - (currenttime - starttime) * m_frequency) // m_frequency)
                    m_ErrorCount = m_ErrorCount + len(m_ProduceError)
                m_Messages = []
                # 一次性发送完剩余所有的消息
                for i in range(0, m_row_count % m_BatchSize):
                    m_Messages.append(self.m_DataWrapper.get_final_string(m_row_struct))
                m_ProduceError = []
                nTotalCount = nTotalCount + self.kafka_Produce(m_TopicName, m_Messages, m_ProduceError)
                m_ErrorCount = m_ErrorCount + len(m_ProduceError)
                if m_ErrorCount != 0:
                    return None, None, None, None, "Total {} messages send to topic {} with {} failed.".\
                        format(nTotalCount, m_TopicName, m_ErrorCount)
                else:
                    return None, None, None, None, "Total {} messages send to topic {} Successful.".\
                        format(nTotalCount, m_TopicName)
            except (KafkaException, KafkaWrapperException) as ke:
                return None, None, None, None, "Failed to send message for topic {}: {}".format(m_TopicName, repr(ke))

        # 逐条读出所有的消息
        matchObj = re.match(r"kafka\s+consume\s+message\s+from\s+topic\s+(.*)\s+to\s+file\s+(.*)\s+group\s+(.*)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            m_FileName = str(matchObj.group(2)).strip()
            m_GroupID = str(matchObj.group(3)).strip()
            f = open(m_FileName, 'w', encoding="utf-8")
            m_bFetchOver = False
            while True:
                for messageList in self.kafka_Consume(m_TopicName, p_szGroupID=m_GroupID):
                    if len(messageList) == 0:
                        m_bFetchOver = True
                        break
                    for line in messageList:
                        print(line, file=f)
                if m_bFetchOver:
                    break
            return None, None, None, None, "Consume completed."

        return None, None, None, None, "Unknown kafka Command."
