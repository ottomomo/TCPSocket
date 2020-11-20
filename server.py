#!/usr/bin/env python
# coding=utf-8

import threading, lock, signal
import SocketServer, socket
import sys, errno
import os
import logging, logging.handlers
import pandas as pd
from pandas import read_csv
'''
    Server.py: script consumidor de datos que escucha las peticiones que llegan a la direccion establecida por las varibales HOST, PORT 
    mediante un socket TCP definido por la clase ThreadedTCPServer. 
    Cada petición se gestiona mediante un nuevo hilo que ejecutará la funcion handle() de la clase RequestHandler. 
    Podrá haber en ejecución tantos hilos como la variable MAX_THREADS indique (incluyendo el hilo principal) en caso de que se llegue
    al limite de hilos las nuevas peticiones se ignorarán. 
    Respecto a los datos consumidos son gestionados por la clase SharedData, esta clase consta de un diccionario (la clave es la cadena, y el valor
    el numero de veces que se ha recibido) donde son almacenados, un semafor lock, el cual gestiona el acceso de los hilos concurrentes al diccionario.
    Cabe destacar que esta clase tambien se encarga de realizar la recuperación y almacenamiento de los datos de forma permanente almacenandolos en el fichero que viene dado por las variables REC_FOLDER+REC_FILE, la escritura de los datos en este se hace en el mismo momento que se actualiza el diccionario, para evitar la pérdida de datos por interrupciones inesperadas.
    Tambien se crea un archivo log en la ruta LOG_FOLDER que registra la comunicacion.
    
    @autor: Octavio Sales
'''

TIMEOUT = 2
HOST = '127.0.0.1'
PORT = 8080

MAX_THREADS = 5

REC_FILE = 'backUp.csv'
REC_FOLDER = './recovery/'
LOG_FOLDER = './log/'
LOG_FILE = 'socket-server.log'
ROTATE_TIME = 'midnight'
LOG_COUNT = 10
log_folder = os.path.dirname(LOG_FOLDER)
rec_folder = os.path.dirname(REC_FOLDER)

# Data Object
class SharedData:
    def __init__(self):
        self.dic={}
        self.lock = threading.Lock()
        self.diff_elems = 0
        self.total_elems = 0
        
    # set the recovery file and read if is not empty
    def recovery(self):
        if os.path.exists(rec_folder+REC_FILE):
            empty= False
            try:
                df = pd.read_csv(REC_FOLDER+REC_FILE)
            except pd.io.common.EmptyDataError:
                empty = True
                logger.info('Recovery file is empty')
            except IOError as e:
                logger.error('I/O error({0}): {1}'.format(e.errno, e.strerror))
                print "I/O error({0}): {1}".format(e.errno, e.strerror)
                print("Exiting...")
                exit(-1)
            if not empty:
                da=df.set_index('key').to_dict()
                self.dic=da['value']
                for k in self.dic: self.total_elems += self.dic[k]
                self.diff_elems = len(self.dic)
                logger.info('total data: %d', self.total_elems)
                logger.info('total different data: %d', self.diff_elems)
        else:
            if not os.path.exists(rec_folder):
                try:
                    os.makedirs(rec_folder)
                except Exception as error:
                    print 'Error creating the rec folder: %s' % error
                    exit()
                    
    # add one data to the dict and to the recovery file using the lock
    def addSafely(self, string):
        self.lock.acquire()
        try:
            if string in self.dic: 
                self.dic[string] +=1
            else: 
                self.dic[string] =1
                self.diff_elems += 1
            self.total_elems += 1
            # write new csv
            df = pd.DataFrame.from_dict(self.dic, orient="index").reset_index()
            df.columns = ['key', 'value']
            pd.DataFrame(df).to_csv('./recovery/backUp.csv', index=None)
        except IOError as e:
            logger.error('I/O error({0}): {1}'.format(e.errno, e.strerror))
            print "I/O error({0}): {1}".format(e.errno, e.strerror)
        finally:
            self.lock.release()
            
    # print all data using the lock
    def getSafely(self):
        self.lock.acquire()
        try:
            print("Total elements: "+str(self.total_elems))
            print("Different elements: "+str(self.diff_elems))
            for k in self.dic: print(str(k)+": "+str(self.dic[k]))
        finally:
            self.lock.release()
            
    # release the lock if is locked by the thread
    def releaseSafely(self):
        if self.lock.locked(): self.lock.release()
    
    # check if is valid data
    def validData(self, data):
        if len(data) == 5 and data.isupper: return True
        else: return False
            
# thread request Handle
class RequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            # chequeamos el numero de threads activos (incluye el main thread). Si es mayor que el limite establecido cerramos la conexion
            # y no atendemos al cliente. Lo trazamos
            if threading.activeCount() > MAX_THREADS:
                logger.warn('%s -- Execution threads number: %d', threading.currentThread().getName(),
                            threading.activeCount() - 1)
                logger.warn('Max threads number has been reached.')
                #self.closed()
            # si no hemos alcanzado el limite lo atendemos
            else:
                threadName = threading.currentThread().getName()
                activeThreads = threading.activeCount() - 1
                clientIP = self.client_address[0]
                logger.info('[%s] -- New connection from %s -- Active threads: %d' , threadName, clientIP, activeThreads)
                while True:
                    data = self.request.recv(5)
                    if not data: 
                        logger.warn('[%s] -- Socket connection from %s broken', threadName, clientIP)
                        break
                    logger.info('[%s] -- %s -- Received: %s' , threadName, clientIP, data)
                    if shData.validData(data):
                        shData.addSafely(data)
                        data = self.request.send('ack')
                    else:
                        data = self.request.send('incorrect')
                    if not data: 
                        logger.warn('[%s] -- Socket connection from %s broken', threadName, clientIP)
                        break
                shData.getSafely()
        except Exception as error:
            shData.releaseSafely()
            if str(error) == "timed out":
                logger.error ('[%s] -- %s -- Timeout on data transmission ocurred after %d seconds.' ,threadName, clientIP, TIMEOUT)

# socket server
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

    def finish_request(self, request, client_address):
        threadName = threading.currentThread().getName()
        activeThreads = threading.activeCount() - 1
        request.settimeout(TIMEOUT)
        SocketServer.TCPServer.finish_request(self, request, client_address)
        SocketServer.TCPServer.close_request(self, request)
        logger.info('[%s] -- Connection closed from %s ' , threadName, client_address[0])

# shutdownHandler
def shutdownHandler(msg, evt):
    print "shutdown handler called"
    server.shutdown()
    print "shutdown complete"
    evt.set()
# signal handler        
def signal_handler(signum, frame):
    t = threading.Thread(target = shutdownHandler,args =('SIGTERM', doneEvent))
    t.start()

if __name__ == '__main__':
    # set up the logger
    if not os.path.exists(log_folder):
        try:
            os.makedirs(log_folder)
        except Exception as error:
            print 'Error creating the log folder: %s' % error
            exit()

    try:
        logger = logging.getLogger('socket-server')
        loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG_FOLDER + LOG_FILE , ROTATE_TIME, 1, backupCount=LOG_COUNT)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        loggerHandler.setFormatter(formatter)
        logger.addHandler(loggerHandler)
        logger.setLevel(logging.DEBUG)
    except Exception as error:
        print '------------------------------------------------------------------'
        print '[ERROR] Error writing log at %s: %s' % (LOG_FOLDER, error)
        print '[ERROR] Please verify path folder exits and write permissions'
        print '------------------------------------------------------------------'
        exit()

    print("Welcome to server.py")
    print("[INFO]: The recovery file is in the path: "+REC_FOLDER+REC_FILE)
    print("[INFO]: The log file is in the path: "+LOG_FOLDER + LOG_FILE)
    doneEvent = threading.Event()
    shData = SharedData()
    shData.recovery()
    try:
        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        print "Starting server TCP at IP %s and port %d..." % (HOST,PORT)
        server = ThreadedTCPServer((HOST, PORT), RequestHandler)
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        server.serve_forever()
        doneEvent.wait()
        print('Waiting for join threads...')
        th=threading.enumerate()
        for i in th: 
            if i.isAlive() and threading.currentThread().ident != i.ident: i.join()
        
    except socket.error as error:
        if error.errno == errno.ECONNREFUSED:
            print(os.strerror(error.errno))
            logger.error('%s', os.strerror(error.errno))
        else:
            raise
    finally:
        server.server_close()
