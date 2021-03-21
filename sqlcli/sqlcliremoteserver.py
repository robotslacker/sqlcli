from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import random
import string
from .sqlcli import SQLCli
from .sqlcliexception import SQLCliException

app = FastAPI()
sga = {}             # clientid: [(connection, cursor),]


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

        return {"clientid": m_ClientID}

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
    @app.post("/DoCommand")
    def Process_CommandRequest(p_RequestData: CommandData):
        if p_RequestData.clientid in sga.keys():
            try:
                if p_RequestData.op == "execute":
                    m_PostResult = []
                    m_SQLCli = sga[p_RequestData.clientid]
                    for title, cur, headers, columntypes, status in \
                            m_SQLCli.SQLExecuteHandler.run(p_RequestData.command):
                        m_PostResult.append((title, cur, headers, columntypes, status))
                    sga[p_RequestData.clientid] = m_SQLCli
                    return {
                        "ret": 0,
                        "dataset": m_PostResult
                    }
                else:
                    return {"ret": -1,
                            "message": "Unknown op [" + p_RequestData.op + "] command."}
            except SQLCliException as se:
                return {"ret": -1,
                        "message": se.message}
        else:
            return {"ret": -1, "message": "clientid [" + p_RequestData.clientid + "] does not exist."}

    @staticmethod
    def Start_SQLCliServer(p_ServerPort):
        uvicorn.run(app=app, host="0.0.0.0", port=p_ServerPort)
