#!/usr/bin/env python3
# ===========================================================================
# podips-writer: A program that reads logs from a queue and send to Fluentd 
# @author Flávio Gomes da Silva Lisboa <flavio.lisboa@fgsl.eti.br>
# @license LGPL-2.1
# ===========================================================================
from fluent import sender
import json
import os
import requests
import socket
import stomp
import time

# connection to Fluentd
def getLogger():
    if  os.getenv('LOG_SERVER') is not None:
        log_server = os.getenv('LOG_SERVER')
    else:
        try:
            with open("log_server", "r") as f:
                log_server = f.read()
        except IOError:
             log_server = ""

    if  os.getenv('LOG_SERVER_PORT') is not None:
        log_server_port = int(os.getenv('LOG_SERVER_PORT'))
    else:
        log_server_port = 24224 

    if  os.getenv('LOG_TAG') is not None:
        log_tag = os.getenv('LOG_TAG')
    else:
        log_tag = "audit"  

    try: 
        logger = sender.FluentSender( log_tag, host=log_server, port=log_server_port )
        print("Connected to Fluentd")
        return logger
    except:
        quit("ERROR: could not open connection to Fluentd server."); 

# connection to ActiveMQ
def getQueue():
    print("Loading queue configuration...")
    try:
        with open("queue.config.json", "r") as f:
            queue_config = json.loads(f.read())
    except IOError:
        queue_config = {"QUEUE_USERNAME" : "", "QUEUE_PASSWORD": "", "QUEUE_HOST": "", "QUEUE_PORT": ""}

    print("ActiveMQ configuration loaded from JSON file...")

    if  os.getenv('QUEUE_USERNAME') is not None:
        queue_username = os.getenv('QUEUE_USERNAME')
    else:
        queue_username = queue_config['QUEUE_USERNAME']
    print("queue_username defined...")

    if  os.getenv('QUEUE_PASSWORD') is not None:
        queue_password = os.getenv('QUEUE_PASSWORD')
    else:
        queue_password = queue_config['QUEUE_PASSWORD']
    print("queue_password defined...")

    if  os.getenv('QUEUE_HOST') is not None:
        queue_host = os.getenv('QUEUE_HOST')
    else:
        queue_host = queue_config['QUEUE_HOST']    
    print("queue_host defined...")

    if  os.getenv('QUEUE_PORT') is not None:
        queue_port = int(os.getenv('QUEUE_PORT'))
    else:
        queue_port = int(queue_config['QUEUE_PORT'])
    print("queue_port defined...")

    print("Trying to connect with queue using " + queue_host + ":" + str(queue_port) + "...")
    queue = stomp.Connection([(queue_host,queue_port)])
    queue.connect(queue_username, queue_password, wait=True)
    print("Connected to queue using user " + queue_username + " in host " + queue_host + ":" + str(queue_port))
    queue.set_listener('logger',     LoggerListener())
    queue.subscribe('/queue/pods',1)
    print("Queue /queue/pods subscribed")
    return queue  

def getPodipsHost():
        if os.getenv('PODIPS_HOST') is not None:
            podips_host = os.getenv('PODIPS_HOST')
        else:
            try:
                with open("podips_host", "r") as f:
                    podips_host = f.read()
            except IOError:
                podips_host = ""
        return podips_host
    
class LoggerListener(stomp.ConnectionListener):
    def on_error(self, frame, args):
        if hasattr(frame, 'body'):
            print('ERROR: received an error "%s"' % frame.body)
        else:
            print('ERROR: when tried to read messages')
        try:    
            r = requests.get(getPodipsHost() + "/queue/read/500/fail", verify=False)
            if r.status_code != 200:
                print("ERROR: could not send status of queue reading")
        except Exception as e:
            print("ERROR: could not send status of queue reading:",e)
        if os.path.exists("/tmp/queue_status"):
            os.remove("/tmp/queue_status")
    def on_message(self, frame, args):
        podips_host = getPodipsHost();
        try:
            r = requests.get(podips_host + "/queue/read/200/success", verify=False)
            if r.status_code != 200:
                print("ERROR: could not send status of queue reading")
        except Exception as e:
            print("ERROR: could not send status of queue reading:",e)
        if hasattr(frame,'body'):
            message = frame.body
        else:
            message = args
        print('PODIPS-WRITER: received a message "%s"' % message)
        try:
             f = open("/tmp/queue_status", "w")
             f.write("working") 
             f.close()
        except IOError:
             print("ERROR: could not create queue status file")
        try:
            log = json.loads(message)
            print("PODIPS-WRITER: message loaded as JSON")
            logger = getLogger()
            if not logger.emit('pod_ip', log):                
                print("ERROR: when try sent to Fluentd:")
                print(logger.last_error)
                logger.clear_last_error()
                try:
                    r = requests.get(podips_host + "/log/500/fail", verify=False)
                    if r.status_code != 200:
                        print("ERROR: could not send status of log writing")
                except Exception as e:
                    print("ERROR: could not send status of log writing:",e)
                if os.path.exists("/tmp/log_status"):
                    os.remove("/tmp/log_status")
            else:
                try:
                    r = requests.get(podips_host + "/log/200/success", verify=False)
                    if r.status_code != 200:
                        print("ERROR: could not send status of log writing")
                except Exception as e:
                    print("PODIPS-WRITER: Log sent to Fluentd:",e)
                try:
                    f = open("/tmp/log_status", "w")
                    f.write("working") 
                    f.close()
                except IOError:
                    print("ERROR: could not create log status file")
            logger.close()
        except Exception as e:
            print("ERROR: could not send log...:",e);

# # # # # 
# # # #
# # # Main function
# # 
# 

print("Waiting 10 seconds to start...")
time.sleep(10)

while True:
    try:
        queue = getQueue();
        print("Waiting for messages...")
        time.sleep(60)
        queue.disconnect();
    except Exception as e:
        print("ERROR:",e)

print("Program finished unexpectedly... and unexplainedly")
