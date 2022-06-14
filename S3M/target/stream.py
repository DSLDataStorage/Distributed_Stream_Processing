import time
import socket

URL = '192.168.0.10'
PORT = 9999

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((URL,PORT))
sock.listen(1)
conn, addr = sock.accept()

while True:
    with open('./ucr_seq.csv') as file:
        for line in file.readlines():
            conn.send(line.encode())
            time.sleep(0.016)
    time.sleep(2)

conn.close()
sock.close()
