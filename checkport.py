import socket

def check_port(port: int) -> bool:
    opened = False
    try:
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.bind(('127.0.0.1', 8080))
        sock.listen(5)

        sock.close()

    except socket.error as e :
        pass
    else:
        opened = True
    return opened

for i in range(1024,65536):
    with open('port.txt','at') as f:
        f.write(f'порт {i} открыт - {check_port(i)}')
