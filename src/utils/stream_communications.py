def receive_header(client_sock):
    complete_header = ""
    extra_payload = ""
    while True:
        partial_header = client_sock.recv(4).decode('utf-8')
        if not partial_header:
            print("Error reading chunk header")
            break
        if '|' in partial_header:
            complete_header += partial_header.split('|')[0]
            extra_payload = partial_header.split('|')[1]
            break
        complete_header += partial_header
    return complete_header, extra_payload

def receive_msg(client_sock):
    header, extra_payload = receive_header(client_sock)
    if not header:
        return ""
    expected_payload_size = int(header)
    complete_msg = header + '|' + extra_payload
    bytes_received = len(extra_payload)
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
