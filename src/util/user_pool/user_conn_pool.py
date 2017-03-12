#!/usr/bin/python
#encoding:utf-8
import threading as _threading

class UserConnPool():
    
    def __init__(self):
        self.mutex = _threading.Lock()
        self.participants = {}
        
    def get(self, user_id):
        conn_list = []
        self.mutex.acquire()
        conn_list = list(self.participants.get(user_id, []))
        self.mutex.release()
        return conn_list
    
    def add(self, user_id, conn):
        self.mutex.acquire()
        try:
            if self.participants.get(user_id, ""):
                self.participants[user_id].add(conn)
            else:
                self.participants[user_id] = set([conn])
        finally:
            self.mutex.release()
        return True
    
    def remove(self, user_id, conn):
        self.mutex.acquire()
        try:
            self.participants[user_id].remove(conn)
            if not self.participants[user_id]:
                self.participants.pop(user_id)
        finally:
            self.mutex.release()
        return True
    
    def user_conn_num(self, user_id):
        self.mutex.acquire()
        num = len(self.participants.get("user_id", []))
        self.mutex.release()
        return num
    
    def user_conn_overview(self):
        self.mutex.acquire()
        user_len_dict = {}
        for user_id in self.participants:
            user_len_dict[user_id] = len(self.participants[user_id])
        self.mutex.release()
        return user_len_dict
    
    
        
        
