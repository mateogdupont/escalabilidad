import pickle

def receive_header(client_sock):
    complete_header = b""
    extra_payload = b""
    while True:
        partial_header = client_sock.recv(4)
        if not partial_header:
            print("Error reading chunk header")
            break
        if b'|' in partial_header:
            complete_header += partial_header.split(b'|')[0]
            extra_payload = partial_header.split(b'|')[1]
            break
        complete_header += partial_header
    return complete_header, extra_payload

def receive_msg(client_sock):
    header, extra_payload = receive_header(client_sock)
    if not header:
        return ""
    expected_payload_size = int(header.decode('utf-8'))
    received_data = extra_payload
    while len(received_data) < expected_payload_size:
        chunk = client_sock.recv(min(expected_payload_size - len(received_data), 2048))
        if chunk == b'':
            raise RuntimeError("Socket connection broken")
        received_data += chunk
    return pickle.loads(received_data)


def send_msg(socket, msg):
    msg = pickle.dumps(msg)
    msg = str(len(msg)).encode('utf-8') + b'|' + msg
    total_sent = 0
    while total_sent < len(msg):
        try:
            sent = socket.send(msg[total_sent:])
        except (OSError, BrokenPipeError):
            print("Error: Broken Pipe")
            break
        if sent == 0:
            print("Error sending data")
            break
        total_sent = total_sent + sent