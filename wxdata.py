#!/usr/bin/python
#
# weewx extension service to collect Loop & Archive packets and share them with other processes
# via a python Multiprocessing connection
#
# Copyright (c) 2015 William B. Phelps <wm@usa.net>
# See the file LICENSE.txt for your full rights.

import weewx
import weewx.engine
import weeutil.weeutil

from multiprocessing.connection import Listener
from multiprocessing import Event
import threading

import os, sys, signal
import time
import syslog
import socket

# global dictionary variables shared with Server thread 
# main thread replaces variable to update, server thread only reads it
# atomic operations should be safe

loop_data = { 'noData':None }
archive_data = { 'noData':None }
lock = threading.Lock()

class Server(threading.Thread):
    def __init__(self, name, address, key):
        threading.Thread.__init__(self)
        self.name = name
        self.address = address
        self.key = key
        self.exit = Event()
#        self.listener = Listener(self.address, authkey=self.key)

    def run(self):
        global loop_data, archive_data, conn, lock
        syslog.syslog(syslog.LOG_INFO, "wxdata server started")
        syslog.syslog(syslog.LOG_INFO, "wxdata: server process {}".format(os.getpid()))
        syslog.syslog(syslog.LOG_INFO, "wxdata: address: '{}' ".format(self.address))
        syslog.syslog(syslog.LOG_INFO, "wxdata: key: '{}' ".format(self.key))
        listener = Listener(self.address, authkey=self.key)
        listener._listener._socket.settimeout(10)
        while True:
          # try to accept a connection - most likely failure is a timeout
          try:
            conn = listener.accept()
            if self.exit.is_set():
              syslog.syslog(syslog.LOG_INFO, "wxdata: server exiting")
              break
#            syslog.syslog(syslog.LOG_INFO, "wxdata: connection accepted from: " + format(listener.last_accepted))
            req = conn.recv()
            if isinstance(req,str): # simple request?
              if req == "wxloop":
                with lock:  data = loop_data
              elif req == "wxarchive":
                with lock:  data = archive_data
              elif req == "wxname":
                data = self.name  # station name as string
              conn.send(data)
            elif isinstance(req,tuple): # request with keys?
              keys = req[1:] # extract the key list
              req = req[0]
              with lock:
                if req == "wxloop":
                  wxdata = loop_data
                elif req == "wxarchive":
                  wxdata = archive_data
#                data = {k: wxdata.get(k,'**BadKey**') for k in keys}
              dp = wxdata.get('dewpoint','***')
              if dp == "***":
                syslog.syslog(syslog.LOG_ERR, "wxdata: dewpoint missing".format(len(loop_data)))
              data = {k: wxdata.get(k,None) for k in keys}
              conn.send(data)
            conn.close()
          except socket.timeout:
            pass
          except Exception as e:
            syslog.syslog(syslog.LOG_ERR, "wxdata: Exception")
            syslog.syslog(syslog.LOG_ERR, "wxdata: error: {0} - {1}".format(type(e), e))
#            syslog.syslog(syslog.LOG_ERR, "wxdata: error: {}".format(sys.exc_info()[0]))
        listener.close()
        syslog.syslog(syslog.LOG_INFO, "wxdata: server ended")

    def shutdown(self):
        syslog.syslog(syslog.LOG_INFO, "wxdata: server shutdown")
        self.exit.set()

class WXDATA(weewx.engine.StdService):

    def __init__(self, engine, config_dict):
        super(WXDATA, self).__init__(engine, config_dict)
        d = config_dict.get('WXDATA', {})
        self.name = d.get('name', 'WX Data')
        self.address = d.get('address', '')
        self.port = int(d.get('port', 6000))
        self.key = d.get('key', 'wxdata key')
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.handle_new_archive)
        self.bind(weewx.NEW_LOOP_PACKET, self.handle_new_loop)
        syslog.syslog(syslog.LOG_INFO, "wxdata: startup")

        global server, lock
        server = Server(self.name, (self.address, self.port), self.key)
        server.daemon = True # kill thread when we exit
        server.start()

    def handle_new_loop(self, event):
        global loop_data
        with lock:
          loop_data = event.packet
#          dp = loop_data.get('dewpoint','***')
#          if dp == "***":
#        syslog.syslog(syslog.LOG_ERR, "wxdata: loop_data len={}".format(len(loop_data)))
#        syslog.syslog(syslog.LOG_INFO, "wxdata: {}".format(loop_data)[0:120])

    def handle_new_archive(self, event):
        global archive_data
        delta = time.time() - event.record['dateTime']
        syslog.syslog(syslog.LOG_INFO, "wxdata: archive record, delta {}".format(delta))
        with lock:
          archive_data = event.record

    def shutDown(self):
        global server
        syslog.syslog(syslog.LOG_INFO, "wxdata: shutdown")
        server.shutdown()
        sleep(15) # give it time to shut down
