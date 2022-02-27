# -*- coding: UTF-8 -*-
import re
import pika


class RabbitmqWrapperException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class RabbitmqWrapper(object):

    def __init__(self):
        self.__connection_rabbitmq = None

    def rabbitmq_connect(self, host, port, username, password):
        """
        连接Rabbitmq
        """
        try:
            credentials = pika.PlainCredentials(
                username=username,
                password=password
            )
            self.__connection_rabbitmq = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=host,
                    port=port,
                    credentials=credentials
                )
            )
            self.__channel = self.__connection_rabbitmq.channel()  # 建立频道
        except Exception as e:
            print(e)
            return False

    def rabbitmq_create_queue(self, queue_name, durable=True):
        """
        创建queue
        """
        self.__channel.queue_declare(queue=queue_name, durable=durable)
        return "Queue {} created.".format(queue_name)

    def rabbit_create_exchange(self, exchange_name, exchange_type):
        """
        创建exchange
        """
        self.__channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type)
        return "Exchange {} created.".format(exchange_name)

    def rabbit_queue_bind_exchange(self, queue_name, exchange_name, route_key):
        """
        将queue绑定到exchange
        """
        self.__channel.queue_bind(
            queue=queue_name,
            exchange=exchange_name,
            routing_key=route_key
        )
        return "Bind {} to the exchange {}".format(queue_name, exchange_name)

    def rabbit_queue_unbind_exchange(self, queue_name, exchange_name, route_key):
        """
        将queue和exchange解绑
        """
        self.__channel.queue_unbind(
            queue=queue_name,
            exchange=exchange_name,
            routing_key=route_key
        )
        return "Unbind {} to the exchange {}".format(queue_name, exchange_name)

    def rabbitmq_delete_queue(self, queue_name):
        """
        删除queue
        """
        self.__channel.queue_delete(queue=queue_name)
        return "Queue {} deleted.".format(queue_name)

    def rabbitmq_count_queue_message(self, queue_name):
        queue_message_count = self.__channel.queue_delete(queue=queue_name).method.message_count
        return "Queue message count is {} ".format(queue_message_count)

    def rabbit_delete_exchange(self, exchange_name):
        """
        删除exchange
        """
        self.__channel.exchange_delete(exchange=exchange_name)
        return "Exchange {} deleted.".format(exchange_name)

    def Process_SQLCommand(self, p_szSQL):

        m_szSQL = p_szSQL.strip()

        # rabbitmq connect 192.168.11.95:5672 ldb 123456
        match_obj = re.match(r"rabbitmq\s+connect\s+(.*?)\s+(.*)\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            connect_parameters = match_obj.group(1).split(':')
            connect_host = connect_parameters[0]
            connect_port = connect_parameters[1]
            connect_username = match_obj.group(2).strip()
            connect_password = match_obj.group(3).strip()
            self.rabbitmq_connect(connect_host, connect_port, connect_username, connect_password)
            return None, None, None, None, "Rabbitmq Server connect successful."

        match_obj = re.match(r"rabbitmq\s+create\s+queue\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        # rabbitmq create queue hello
        if match_obj:
            queue_name = match_obj.group(1).strip()
            m_ReturnMessage = self.rabbitmq_create_queue(queue_name=queue_name)
            return None, None, None, None, m_ReturnMessage

        match_obj = re.match(r"rabbitmq\s+create\s+exchange\s+(.*)\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        # rabbitmq create exchange test_hello direct
        if match_obj:
            exchange_name = match_obj.group(1).strip()
            exchange_type = match_obj.group(2).strip()
            m_ReturnMessage = self.rabbit_create_exchange(
                exchange_name=exchange_name,
                exchange_type=exchange_type)
            return None, None, None, None, m_ReturnMessage

        match_obj = re.match(r"rabbitmq\s+queue\s+bind\s+(.*?)\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        # rabbitmq queue bind hello test_hello test_route
        if match_obj:
            queue_name = match_obj.group(1).strip()
            exchange_list = match_obj.group(2).strip().split()
            exchange_name = exchange_list[0]
            if len(exchange_list) == 1:
                routing_key = None
            else:
                routing_key = exchange_list[1]
            m_ReturnMessage = self.rabbit_queue_bind_exchange(
                queue_name=queue_name,
                exchange_name=exchange_name,
                route_key=routing_key)
            return None, None, None, None, m_ReturnMessage

        match_obj = re.match(r"rabbitmq\s+queue\s+unbind\s+(.*)\s+(.*)\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        # rabbitmq queue unbind hello test_hello test_route
        if match_obj:
            queue_name = match_obj.group(1).strip()
            exchange_name = match_obj.group(2).strip()
            routing_key = match_obj.group(3).strip()
            m_ReturnMessage = self.rabbit_queue_unbind_exchange(
                queue_name=queue_name,
                exchange_name=exchange_name,
                route_key=routing_key)
            return None, None, None, None, m_ReturnMessage

        match_obj = re.match(r"rabbitmq\s+delete\s+queue\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        # rabbitmq delete queue hello
        if match_obj:
            queue_name = match_obj.group(1).strip()
            m_ReturnMessage = self.rabbitmq_delete_queue(queue_name=queue_name)
            return None, None, None, None, m_ReturnMessage

        match_obj = re.match(r"rabbitmq\s+delete\s+exchange\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        # rabbitmq delete exchange test_hello
        if match_obj:
            exchange_name = match_obj.group(1).strip()
            m_ReturnMessage = self.rabbit_delete_exchange(exchange_name=exchange_name)
            return None, None, None, None, m_ReturnMessage

        match_obj = re.match(r"rabbitmq\s+count\s+queue\s+message\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        # rabbitmq delete exchange test_hello
        if match_obj:
            queue_name = match_obj.group(1).strip()
            m_ReturnMessage = self.rabbitmq_count_queue_message(queue_name=queue_name)
            return None, None, None, None, m_ReturnMessage

        return None, None, None, None, "Unknown Rabbitmq Command."
