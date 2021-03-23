import uvicorn
import random
import string
import os
import json
from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from .sqlcli import SQLCli


class ConnectionManager:
    def __init__(self):
        # 存放激活的ws连接对象
        self.active_connections: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        # 等待连接
        await ws.accept()
        # 存储ws连接对象
        self.active_connections.append(ws)

    def disconnect(self, ws: WebSocket):
        # 关闭时 移除ws对象
        self.active_connections.remove(ws)

    @staticmethod
    async def send_personal_message(message: str, ws: WebSocket):
        # 发送个人消息
        await ws.send_text(message)

    async def broadcast(self, message: str):
        # 广播消息
        for connection in self.active_connections:
            await connection.send_text(message)


app = FastAPI()
sga = {}             # clientid: SQLCli Instance
manager = ConnectionManager()


class SQLCliRemoteServer:
    class LoginData(BaseModel):
        UserName: str = None
        PassWord: str = None

    class LogoutData(BaseModel):
        clientid: str

    class CommandData(BaseModel):
        clientid: str
        op: str
        command: str = None

    def __init__(self):
        pass

    @staticmethod
    @app.websocket("/DoCommand")
    async def Process_CommandRequest(websocket: WebSocket):
        await manager.connect(websocket)
        try:
            p_RequestData = await websocket.receive_json()
            if "clientid" not in p_RequestData.keys():
                await manager.send_personal_message(
                    json.dumps({
                        "title": "",
                        "cur": "",
                        "headers": "",
                        "columntypes": "",
                        "status": "Parse Command failed. Missed clientid."
                    }),
                    websocket
                )
                manager.disconnect(websocket)
                return

            if p_RequestData["clientid"] in sga.keys():
                if p_RequestData["op"] == "execute":
                    m_SQLCli = sga[p_RequestData["clientid"]]
                    for title, cur, headers, columntypes, status in \
                            m_SQLCli.SQLExecuteHandler.run(p_RequestData["command"]):
                        await manager.send_personal_message(
                            json.dumps({
                                "title": title,
                                "cur": cur,
                                "headers": headers,
                                "columntypes": columntypes,
                                "status": status
                            }),
                            websocket
                        )
                manager.disconnect(websocket)
        except WebSocketDisconnect as we:
            if we.code == 1000:
                print("Query Completed Successful.")
            else:
                print("Query Completed Unexpected with code [." + str(we.code) + "]")

    @staticmethod
    @app.post("/DoLogin")
    def Process_LoginRequest(p_RequestData: LoginData):
        # 用户登录，返回一个随机生成的token
        # m_ClientID = str(uuid.uuid4())
        m_ClientID = ''.join(random.choice(string.digits) for i in range(4))
        m_SQLCli = SQLCli(
            HeadlessMode=True,
            WorkerName=m_ClientID
        )
        m_SQLCli.ClientID = m_ClientID
        sga[m_ClientID] = m_SQLCli

        return {"ret": 0, "clientid": m_ClientID}

    @staticmethod
    @app.post("/DoLogout")
    def Process_LogOut(p_RequestData: LogoutData):
        if p_RequestData.clientid in sga.keys():
            m_SQLCli = SQLCli(sga[p_RequestData.clientid])
            m_SQLCli.exit(cls=None, arg=None)
            sga.pop(p_RequestData.clientid)
            return {"ret": 0}
        else:
            return {"ret": -1, "message": "clientid [" + p_RequestData.clientid + "] does not exist."}

    @staticmethod
    def Start_SQLCliServer(p_ServerPort):
        # 如果定义了RemoteServer配置，取消这个配置
        if "SQLCLI_REMOTESERVER" in os.environ:
            del os.environ['SQLCLI_REMOTESERVER']
        uvicorn.run(app=app, host="0.0.0.0", port=p_ServerPort)
