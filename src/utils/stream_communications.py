def receive_header(client_sock):
    complete_header = ""
    while True:
        partial_header = client_sock.recv(1).decode('utf-8')
        if not partial_header:
            print("Error reading chunk header")
            break
        complete_header += partial_header
        if partial_header == '|':
            break
    return complete_header


def receive_msg(client_sock):
    header = receive_header(client_sock)
    if not header:
        return ""
    expected_payload_size = int(header[:-1])
    complete_msg = header
    bytes_received = 0
    while bytes_received < expected_payload_size:
        partial_msg = client_sock.recv(expected_payload_size - bytes_received)
        if not partial_msg:
            print("Error reading chunk")
            break
        complete_msg += partial_msg.decode('utf-8')
        bytes_received += len(partial_msg)
    return complete_msg.split('|', 1)[1]


def send_msg(socket, msg):
    header = str(len(msg.encode('utf-8'))) + '|'
    msg = header + msg
    remaind_size = len(msg)
    while remaind_size > 0:
        try:
            sent_data_size = socket.send(msg.encode('utf-8'))
        except (OSError, BrokenPipeError):
            print("Error: Broken Pipe")
            break
        if sent_data_size == 0:
            print("Error sending data")
            break
        remaind_size -= sent_data_size
        msg = msg[sent_data_size:]
