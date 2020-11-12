# -*- coding: UTF-8 -*-
import re
import os
import string
import traceback
import random

from .sqlinternal import parse_formula_str, get_final_string
try:
    from confluent_kafka import Producer, Consumer, TopicPartition, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
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

    def Kafka_Connect(self, p_szKafkaServers):
        self.__kafka_servers__ = p_szKafkaServers

    def Kafka_CreateTopic(self, p_szTopicName, p_Partitons=16, p_replication_factor=1):
        a = AdminClient({'bootstrap.servers': self.__kafka_servers__})
        new_topics = [NewTopic(p_szTopicName, num_partitions=p_Partitons, replication_factor=p_replication_factor), ]
        fs = a.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                return "Topic {} created".format(topic)
            except KafkaException as ke:
                if "SQLCLI_DEBUG" in os.environ:
                    print('traceback.print_exc():\n%s' % traceback.print_exc())
                    print('traceback.format_exc():\n%s' % traceback.format_exc())
                return "Failed to create topic {}: {}".format(topic, repr(ke))

    def Kafka_DeleteTopic(self, p_szTopicName):
        a = AdminClient({'bootstrap.servers': self.__kafka_servers__})
        deleted_topics = [p_szTopicName, ]
        fs = a.delete_topics(topics=deleted_topics)
        for topic, f in fs.items():
            try:
                f.result()
                return "Topic {} deleted".format(topic)
            except KafkaException as ke:
                if repr(ke).find("UNKNOWN_TOPIC_OR_PART") != -1:
                    return "Topic {} deleted".format(topic)
                else:
                    if "SQLCLI_DEBUG" in os.environ:
                        print('traceback.print_exc():\n%s' % traceback.print_exc())
                        print('traceback.format_exc():\n%s' % traceback.format_exc())
                return "Failed to drop topic {}: {}".format(topic, repr(ke))

    def kafka_GetOffset(self, p_szTopicName, p_nPartitionID=0, p_szGroupID=''):
        c = Consumer({'bootstrap.servers': self.__kafka_servers__, 'group.id': p_szGroupID, })
        m_OffsetResults = []
        try:
            if p_nPartitionID == -1:
                for pid in c.list_topics(topic=p_szTopicName).topics[p_szTopicName].partitions.keys():
                    tp = TopicPartition(p_szTopicName, pid)
                    (low, high) = c.get_watermark_offsets(tp)
                    m_OffsetResults.append([pid, low, high])
            else:
                tp = TopicPartition(p_szTopicName, p_nPartitionID)
                (low, high) = c.get_watermark_offsets(tp)
                m_OffsetResults.append([p_nPartitionID, low, high])
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
        return message_return

    def Process_SQLCommand(self, p_szSQL):
        m_szSQL = p_szSQL.strip()
        matchObj = re.match(r"create\s+kafka\s+server\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_KafkaServer = str(matchObj.group(1)).strip()
            self.__kafka_servers__ = m_KafkaServer
            return None, None, None, None, "Kafka Server created successful."

        matchObj = re.match(r"create\s+kafka\s+topic\s+(.*)\s+Partitions\s+(\d+)\s+replication_factor\s+(\d+)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            m_PartitionCount = int(matchObj.group(2))
            m_ReplicationFactor = int(matchObj.group(3))
            m_ReturnMessage = self.Kafka_CreateTopic(m_TopicName, m_PartitionCount, m_ReplicationFactor)
            return None, None, None, None, m_ReturnMessage

        matchObj = re.match(r"drop\s+kafka\s+topic\s+(.*)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            m_ReturnMessage = self.Kafka_DeleteTopic(m_TopicName)
            return None, None, None, None, m_ReturnMessage

        # 显示所有Partition的偏移数据
        matchObj = re.match(r"get\s+kafka\s+offset\s+topic(.*)\s+group\s+(.*)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            m_GroupID = str(matchObj.group(2)).strip()
            try:
                m_Results = self.kafka_GetOffset(m_TopicName, -1, m_GroupID)
                m_Header = ["Partition", "minOffset", "maxOffset"]
                m_nTotalOffset = 0
                for m_Result in m_Results:
                    m_nTotalOffset = m_nTotalOffset + int(m_Result[2]) - int(m_Result[1])
                m_Message = "Total " + str(len(m_Results)) + " partitions, total offset is " + str(m_nTotalOffset) + "."
                return None, m_Results, m_Header, None, m_Message
            except KafkaException as ke:
                return None, None, None, None, "Failed to get office for topic {}: {}".format(m_TopicName, repr(ke))

        # 显示所有Partition的偏移数据
        matchObj = re.match(r"get\s+kafka\s+offset\s+topic(.*)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            m_GroupID = 'asdfzxcv1234'    # 一个随机的字符串
            try:
                m_Results = self.kafka_GetOffset(m_TopicName, -1, m_GroupID)
                m_Header = ["Partition", "minOffset", "maxOffset"]
                m_nTotalOffset = 0
                for m_Result in m_Results:
                    m_nTotalOffset = m_nTotalOffset + int(m_Result[2]) - int(m_Result[1])
                m_Message = "Total " + str(len(m_Results)) + " partitions, total offset is " + str(m_nTotalOffset) + "."
                return None, m_Results, m_Header, None, m_Message
            except KafkaException as ke:
                return None, None, None, None, "Failed to get office for topic {}: {}".format(m_TopicName, repr(ke))

        # 从文件中加载消息到Kafka队列中
        matchObj = re.match(r"create\s+kafka\s+message\s+from\s+file\s+(.*)\s+to\s+topic\s+(.*)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_FileName = str(matchObj.group(1)).strip()
            m_TopicName = str(matchObj.group(2)).strip()
            if not os.path.isfile(m_FileName):
                return None, None, None, None, "Failed to load file {}".format(m_FileName)
            with open(m_FileName, 'r', encoding="utf-8") as f:
                m_Messages = f.readlines()
            # 去掉消息中的回车换行符
            for m_nPos in range(0, len(m_Messages)):
                if m_Messages[m_nPos][-1:] == '\n':
                    m_Messages[m_nPos] = m_Messages[m_nPos][:-1]
            m_ProduceError = []
            try:
                nTotalCount = self.kafka_Produce(m_TopicName, m_Messages, m_ProduceError)
                if len(m_ProduceError) != 0:
                    return None, None, None, None, "Total {}/{} messages send to topic {} with {} failed.". \
                        format(nTotalCount, len(m_Messages), m_TopicName, len(m_ProduceError))
                else:
                    return None, None, None, None, "Total {}/{} messages send to topic {} Successful". \
                        format(nTotalCount, len(m_Messages), m_TopicName)
            except (KafkaException, KafkaWrapperException) as ke:
                return None, None, None, None, "Failed to send message for topic {}: {}".format(m_TopicName, repr(ke))

        # 创建topic
        matchObj = re.match(r"create\s+kafka\s+message\s+topic\s+(.*?)\((.*)\)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            m_RawMessages = str(matchObj.group(2)).split('\n')
            m_Messages = []
            for m_nPos in range(0, len(m_RawMessages)):
                if len(m_RawMessages[m_nPos]) != 0:
                    m_Messages.append(get_final_string(parse_formula_str(m_RawMessages[m_nPos])))
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

        matchObj = re.match(r"create\s+kafka\s+message\s+topic\s+(.*?)\((.*)\)(\s+)?rows\s+(\d+)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TopicName = str(matchObj.group(1)).strip()
            m_formula_str = str(matchObj.group(2)).replace('\r', '').replace('\n', '').strip()
            m_row_struct = parse_formula_str(m_formula_str)
            m_row_count = int(str(matchObj.group(4)).strip())
            m_ErrorCount = 0
            nTotalCount = 0
            try:
                for i in range(0, m_row_count // 5000):
                    m_Messages = []
                    for j in range(0, 5000):
                        m_Messages.append(get_final_string(m_row_struct))
                    m_ProduceError = []
                    nTotalCount = nTotalCount + self.kafka_Produce(m_TopicName, m_Messages, m_ProduceError)
                    m_ErrorCount = m_ErrorCount + len(m_ProduceError)
                m_Messages = []
                for i in range(0, m_row_count % 5000):
                    m_Messages.append(get_final_string(m_row_struct))
                m_ProduceError = []
                nTotalCount = nTotalCount + self.kafka_Produce(m_TopicName, m_Messages, m_ProduceError)
                m_ErrorCount = m_ErrorCount + len(m_ProduceError)
                if m_ErrorCount != 0:
                    return None, None, None, None, "Total {} messages send to topic {} with {} failed.".\
                        format(nTotalCount, m_TopicName, m_ErrorCount)
                else:
                    return None, None, None, None, "Total {} messages send to topic {} Successful".\
                        format(nTotalCount, m_TopicName)
            except (KafkaException, KafkaWrapperException) as ke:
                return None, None, None, None, "Failed to send message for topic {}: {}".format(m_TopicName, repr(ke))

        return None, None, None, None, "Unknown kafka Command."
