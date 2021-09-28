import json
import socket

from driver.consumer import Consumer


class NetworkStreamer(Consumer):
    connection: socket.socket

    def __init__(self, host, port, generator):
        self.host = host
        self.port = port
        self.generator = generator

    def update(self, data: []) -> None:
        with self.connection as connection:
            self.send(data, connection)

    def consume_loop(self, consume_f, *args):
        self.generator.start()

        for _ in range(self.generator.get_budget()):
            data = self.generator.next()
            consume_f(json.dumps(data) + '\n', *args)

    def send(self, data, c):
        c.sendall(data.encode())

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as streaming_socket:
            streaming_socket.bind((self.host, self.port))
            streaming_socket.listen(0)

            conn, addr = streaming_socket.accept()

            with conn:
                self.consume_loop(self.send, conn)

            streaming_socket.close()
