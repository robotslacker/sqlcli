# -*- coding: UTF-8 -*-
import re
import redis
from redis.sentinel import Sentinel
from rediscluster import RedisCluster



class RedisWrapperException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class RedisWrapper(object):

    def __init__(self):
        self.name = 'mymaster'
        self.db = 0
        self.type = None
        self.__redisconn = None
        self.host = None
        self.port = None
        self.password = None
        self.startup_nodes = []
        self.sentinel_list = []

    def redis_connect(self, *args):
        """
        连接redis server
        redis server三种模式：standalone, cluster, sentinel
        """
        try:
            if self.type == 'standalone':
                self.__redisconn = redis.Redis(host=self.host, port=self.port, db=self.db, socket_timeout=60)
            elif self.type == 'cluster':
                self.__redisconn = RedisCluster(startup_nodes=self.startup_nodes)
            else:
                self.__redisconn = Sentinel(self.sentinel_list)
                self.master = self.__redisconn.master_for(
                    service_name=self.name,
                    password=self.password,
                    db=self.db
                )
                self.slave = self.__redisconn.slave_for(
                    service_name=self.name,
                    password=self.password,
                    db=self.db
                )
                return self.master, self.slave
        except Exception as e:
            print(e)
            return False

    def redis_set(self, key, value):
        """
        对key设置相应的value值
        """
        if self.type == 'sentinel':
            self.master.setex(name=key, time=1, value=value)
        else:
            self.__redisconn.set(name=key, value=value)
        return "set the key {} to value {}.".format(key, value)

    def redis_get(self, key):
        """
        对key的value进行查询
        """
        if self.type == 'sentinel':
            return self.master.get(key)
        else:
            return self.__redisconn.get(name=key)

    def redis_flushall(self):
        """
        删除所有数据库中的数据
        """
        if self.type == 'sentinel':
            self.master.flushall()
        else:
            self.__redisconn.flushall()
        return "all the keys deleted."

    def Process_SQLCommand(self, p_szSQL):
        m_szSQL = p_szSQL.strip()
        matchObj = re.match(r"redis\s+connect\s+(.*?)\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            self.type = matchObj.group(1).strip()
            m_redis_conn_paramters = matchObj.group(2).split()
            if self.type == 'standalone':
                self.host = m_redis_conn_paramters[0]
                self.port = m_redis_conn_paramters[1]
                self.redis_connect(self.host, self.port)
            elif self.type == 'cluster':
                for ll in range(len(m_redis_conn_paramters)):
                    m_redis_paramter = m_redis_conn_paramters[ll].split(':')
                    self.startup_nodes.append({"host": m_redis_paramter[0], "port": m_redis_paramter[1]})
                self.redis_connect(self.startup_nodes)
            else:
                for ll in range(len(m_redis_conn_paramters)):
                    m_redis_paramter = m_redis_conn_paramters[ll].split(':')
                    self.sentinel_list.append((m_redis_paramter[0], int(m_redis_paramter[1])))
                self.redis_connect(self.sentinel_list)
            return None, None, None, None, "Redis Server connect successful."

        matchObj = re.match(r"redis\s+set\s+(.*?)\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_data_name = str(matchObj.group(1)).strip()
            m_data_value = str(matchObj.group(2)).strip()
            m_ReturnMessage = self.redis_set(m_data_name, m_data_value)
            return None, None, None, None, m_ReturnMessage

        matchObj = re.match(r"redis\s+get\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_data_name = str(matchObj.group(1)).strip()
            m_ReturnMessage = str(self.redis_get(m_data_name))
            return None, None, None, None, m_ReturnMessage

        matchObj = re.search(r"redis\s+flushall", m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_ReturnMessage = str(self.redis_flushall())
            return None, None, None, None, m_ReturnMessage

        return None, None, None, None, "Unknown Redis Command."
