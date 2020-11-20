#!/usr/bin/env python
# coding=utf-8
import socket, signal, os, time, logging, errno
import string, random
'''
    Client.py: script generador de datos que envia al destino establecido por las varibales SERVER, PORT mediante un socket TCP.
    En el momento que se conecta al servidor se establece una alarma en 60 segundo que terminará el proceso psado ese tiempo.
    Los datos generados vienen dados por la función gen().
    Se crea un archivo log en la ruta relativa: './log/client.log' que registra la comunicacion.
    
    @autor: Octavio Sales
'''
SERVER='127.0.0.1'
PORT = 8080
LOG_FILE = './log/client.log'

# alarm handler
def alarm(signum, stack):
    print('Alarm received. Exiting...:')
    logger.info('Close connection')
    client.shutdown(socket.SHUT_WR)
    client.close()
    exit()

# data generator
def gen(size=5, chars=string.ascii_uppercase):
    return ''.join(random.choice(chars) for _ in range(size))



if __name__ == '__main__':
    try:
        # set up logger
        logger = logging.getLogger('client')
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler('./log/client.log')
        fh.setLevel(logging.DEBUG)
        logger.addHandler(fh)
        formatter = logging.Formatter('%(asctime)s - %(process)d - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except Exception as error:
        print ('[ERROR] Error writing log at %s: %s' % (LOG_FILE, error))
        exit()
    print("Welcome to client.py")
    print("[INFO]: The log file is in the path: "+LOG_FILE)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect((SERVER, PORT))
        logger.info('Connected to server')
        print("Connected to server")
        # Call alarm in 60 seconds
        signal.signal(signal.SIGALRM, alarm)
        signal.alarm(60)

        while True:
            # generate data
            data =  gen()
            sent = client.send(data)
            if not sent: 
                print("Socket connection broken. Exiting...")
                logger.error('Socket connection broken')
                break
            logger.info('%s -- sent, waitting for response...', data)
            response = client.recv(10)
            if not response: 
                print("Socket connection broken. Exiting...")
                logger.error('Socket connection broken')
                break
            elif response == 'ack':
                logger.info('%s -- sent confirmed', data)
            else:
                logger.warn('%s -- no valid data sent', data)
    except socket.error as error:
        print(os.strerror(error.errno))
        logger.error('%s', os.strerror(error.errno))
        print("Exiting")
    except KeyboardInterrupt:
        logger.warn('KeyboardInterrupt')
    finally:
        logger.info('Close connection')
        client.close()
