# -*- coding: UTF-8 -*-
import os
import re
import json
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError


class NewKafkaWrapper(object):
    def __init__(self):
        self.client = None
        self.n_bootstrap_servers = None
        self.__mLocalRootDirectory = None  # 本地文件的初始目录

    def nkafka_connect(self):
        self.client = KafkaAdminClient(bootstrap_servers=self.n_bootstrap_servers)

    def nkafka_create_topic(self, n_topic_name, n_num_partitions, n_replication_factor, timeout_ms=1000):
        topic_list = []
        # 创建自定义分区的topic，创建名称为test，12个分区3个副本的topic
        topic_list.append(NewTopic(
            name=n_topic_name,
            num_partitions=n_num_partitions,
            replication_factor=n_replication_factor,
            replica_assignments=None,
            topic_configs=None))
        for _ in range(5):
            try:
                self.client.create_topics(new_topics=topic_list, timeout_ms=timeout_ms, validate_only=False)
            except KafkaError as ke:
                if ke.__class__.__name__ == "TopicAlreadyExistsError":
                    self.nkafka_delete_topic(n_topic_name)
                    time.sleep(1)
                    continue
                return "Failed to create topic {}: {}".format(n_topic_name, repr(ke))
            else:
                break
        return "Topic {} created.".format(n_topic_name)

    def nkafka_delete_topic(self, n_topic_name, timeout_ms=1000):
        for _ in range(5):
            if n_topic_name in self.nkafka_get_all_topics():
                self.client.delete_topics(topics=[n_topic_name], timeout_ms=timeout_ms)
                time.sleep(1)
            else:
                break
        return "Topic {} deleted.".format(n_topic_name)

    def nkafka_get_all_topics(self):
        return self.client.list_topics()

    def nkafka_produce(self, n_topic_name, n_message_list):
        # produce asynchronously
        n_count = len(n_message_list)
        producer = KafkaProducer(bootstrap_servers=[self.n_bootstrap_servers])
        for index in range(n_count):
            producer.send(n_topic_name, n_message_list[index].encode('utf-8'))
        # block until all async messages are sent
        producer.flush()
        return n_count

    def nkafka_produce_json(self, n_topic_name, json_data):
        n_count = len(json_data.keys())
        producer = KafkaProducer(bootstrap_servers=[self.n_bootstrap_servers],
                                 value_serializer=lambda m: json.dumps(m).encode('ascii'))
        producer.send(n_topic_name, json_data)
        return n_count

    def nkafka_comsumer(self, n_topic_name, group_id):
        consumer = KafkaConsumer(n_topic_name,
                                 group_id=group_id,
                                 auto_offset_reset='earliest',
                                 bootstrap_servers=[self.n_bootstrap_servers])
        message_list = []
        for message in consumer:
            message_list.append(message.value.decode('utf-8'))
        return message_list

    # 切换本地的文件目录
    def SQLScript_LCD(self, p_szPath):
        self.__mLocalRootDirectory = p_szPath

    def Process_SQLCommand(self, p_szSQL):
        m_szSQL = p_szSQL.strip()

        matchObj = re.match(r"nkafka\s+connect\s+server\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            self.n_bootstrap_servers = str(matchObj.group(1)).strip()
            self.nkafka_connect()
            print('connecting ...')
            return None, None, None, None, "Kafka Server set successful."

        matchObj = re.match(r"nkafka\s+create\s+topic\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            n_topic_name = str(matchObj.group(1)).strip()
            n_num_partitions = 16  # 默认的Kafka分区数量
            n_replication_factor = 1  # 默认的副本数量
            n_ReturnMessage = self.nkafka_create_topic(n_topic_name, n_num_partitions, n_replication_factor)
            return None, None, None, None, n_ReturnMessage

        matchObj = re.match(r"nkafka\s+delete\s+topic\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            n_topic_name = matchObj.group(1)
            # topic_list = [n_topic_name]
            n_ReturnMessage = self.nkafka_delete_topic(n_topic_name)
            return None, None, None, None, n_ReturnMessage

        matchObj = re.search(r"nkafka\s+get\s+all\s+topics",
                             m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            n_ReturnMessage = self.nkafka_get_all_topics()
            return None, None, None, None, str(n_ReturnMessage)

        matchObj = re.match(r"nkafka\s+produce\s+message\s+topic\s+(.*?)\((.*)\)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            n_topic_name = str(matchObj.group(1)).strip()
            n_messge_list = matchObj.group(2).strip().split('\n')
            n_total_count = self.nkafka_produce(n_topic_name, n_messge_list)
            return None, None, None, None, "Total {} messages send to topic {} Successful". \
                format(n_total_count, n_topic_name)

        matchObj = re.search(r"nkafka\s+produce\s+message\s+from\s+file\s+(.*?)\s+to\s+topic\s+(.*)$",
                             m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            n_file_name = str(matchObj.group(1)).strip()
            n_topic_name = str(matchObj.group(2)).strip()
            if self.__mLocalRootDirectory is not None:
                mLocalPath = os.path.join(str(self.__mLocalRootDirectory), n_file_name)
            else:
                mLocalPath = n_file_name
            with open(mLocalPath, 'r', encoding="utf-8") as f:
                m_MessageList = []
                for line in f:
                    m_MessageList.append(line.strip('\n'))
            n_total_count = self.nkafka_produce(n_topic_name, m_MessageList)
            return None, None, None, None, "Total {} messages send to topic {} Successful". \
                format(n_total_count, n_topic_name)

        matchObj = re.search(r"nkafka\s+produce\s+json\s+message\s+from\s+file\s+(.*?)\s+to\s+topic\s+(.*)$",
                             m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            n_jsonfile_name = str(matchObj.group(1)).strip()
            n_topic_name = str(matchObj.group(2)).strip()
            if self.__mLocalRootDirectory is not None:
                mLocalPath = os.path.join(str(self.__mLocalRootDirectory), n_jsonfile_name)
            else:
                mLocalPath = n_jsonfile_name
            if not mLocalPath.endswith('.json'):
                return None, None, None, None, "Please provide a json file."
            with open(mLocalPath, 'r', encoding="utf-8") as f:
                json_data = json.load(f)
            n_total_count = self.nkafka_produce_json(n_topic_name, json_data)
            return None, None, None, None, "Total {} messages send to topic {} Successful". \
                format(n_total_count, n_topic_name)
