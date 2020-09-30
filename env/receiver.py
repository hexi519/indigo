# Copyright 2018 Francis Y. Yan, Jestin Ma
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.


import sys
import json
import socket
import select
import datagram_pb2
import project_root
from helpers.helpers import READ_FLAGS, ERR_FLAGS, READ_ERR_FLAGS, ALL_FLAGS


class Receiver(object):
    def __init__(self, ip, port):
        self.peer_addr = (ip, port)

        # UDP socket and poller
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)    #TODO 怎么发的是UDP的包...能保证可靠传输么...
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 

        self.poller = select.poll()
        self.poller.register(self.sock, ALL_FLAGS)  # 注册当前这个文件描述符，用于监听

    def cleanup(self):
        self.sock.close()

    # 发送第二次握手
    def construct_ack_from_data(self, serialized_data):
        """Construct a serialized ACK that acks a serialized datagram."""

        data = datagram_pb2.Data()  #* protobuf有空可以了解下，这个就是里面一系列的
        data.ParseFromString(serialized_data)

        # 把所有来的信息都放到ack里面了，也许是为了方便记录
        ack = datagram_pb2.Ack()
        ack.seq_num = data.seq_num
        ack.send_ts = data.send_ts      # send_ts是什么
        ack.sent_bytes = data.sent_bytes
        ack.delivered_time = data.delivered_time
        ack.delivered = data.delivered
        ack.ack_bytes = len(serialized_data)

        return ack.SerializeToString()
    
    # 发送第一次握手
    def handshake(self):
        """Handshake with peer sender. Must be called before run()."""
        #? 网络的基础还需要补一补2333
        self.sock.setblocking(0)  # non-blocking UDP socket

        TIMEOUT = 1000  # ms

        retry_times = 0
        self.poller.modify(self.sock, READ_ERR_FLAGS)   # 除了select.POLLOUT以外的时间都监听233  （ 注册的时候是ALL_FLAGS

        while True:
            self.sock.sendto('Hello from receiver', self.peer_addr)
            events = self.poller.poll(TIMEOUT)

            if not events:  # timed out
                retry_times += 1
                if retry_times > 10:
                    sys.stderr.write(
                        '[receiver] Handshake failed after 10 retries\n')
                    return
                else:
                    sys.stderr.write(
                        '[receiver] Handshake timed out and retrying...\n')
                    continue

            for fd, flag in events:
                assert self.sock.fileno() == fd

                if flag & ERR_FLAGS:
                    sys.exit('Channel closed or error occurred')

                if flag & READ_FLAGS:
                    msg, addr = self.sock.recvfrom(1600)  # get return of(msg,addr) data是包含接收数据的字符串，address是发送数据的套接字地址

                    if addr == self.peer_addr:
                        if msg != 'Hello from sender':
                            # 'Hello from sender' was presumably lost
                            # received subsequent data from peer sender
                            ack = self.construct_ack_from_data(msg)
                            if ack is not None:
                                self.sock.sendto(ack, self.peer_addr)
                        return

    # 发了第二次握手包之后就开始keep收包
    def run(self):
        self.sock.setblocking(1)  # blocking UDP socket  # TODO 这里又设置成blocking的了...

        while True:
            serialized_data, addr = self.sock.recvfrom(1600)

            if addr == self.peer_addr:
                ack = self.construct_ack_from_data(serialized_data)
                if ack is not None:
                    self.sock.sendto(ack, self.peer_addr)
