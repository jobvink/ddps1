import json
import socket
import socket
from typing import Tuple

from driver.consumer import Consumer


class NetworkStreamer(Consumer):
    connection: socket.socket

    def __init__(self, host, port, generator):
        self.host = host
        self.port = port
        self.generator = generator
        # self.stream_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.stream_socket.settimeout(100)
        # self.stream_socket.bind(('localhost', port))
        # self.stream_socket.listen(0)
        # self.connection, _ = self.stream_socket.accept()

    # def send(self, data: Tuple[int, int, float, float], connection):
    #     purchase = {'userID': int(data[0]), 'packID': int(data[1]), "price": data[2], "event_time": data[3]}
    #     encoded_data = json.dumps(purchase).encode()
    #     connection.send(encoded_data)

    def update(self, data: []) -> None:
        with self.connection as connection:
            self.send(data, connection)

    # generates `self.budget` number of tuples and consumes them using the callable `consume_f` argument
    def consume_loop(self, consume_f, *args):
        self.generator.start()

        for _ in range(self.generator.get_budget()):
            data = self.generator.next()
            consume_f(json.dumps(data) + '\n', *args)

    def send(self, data, c):
        c.sendall(data.encode())

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as streaming_socket:
            # s.settimeout()
            streaming_socket.bind((self.host, self.port))
            streaming_socket.listen(0)

            conn, addr = streaming_socket.accept()

            with conn:
                self.consume_loop(self.send, conn)

            streaming_socket.close()
