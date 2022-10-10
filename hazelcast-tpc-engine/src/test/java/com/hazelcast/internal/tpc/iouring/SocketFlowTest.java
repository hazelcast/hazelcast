/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.tpc.iouring;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;

// crappy test.
public class SocketFlowTest {

    private ServerSocket serverSocket;
    private NativeSocket socket;

    @After
    public void after() {
        closeQuietly(serverSocket);
        closeQuietly(socket);
    }

    @Test
    public void test() throws IOException {
        int port = 5000;
        serverSocket = new ServerSocket(port);
        ServerThread serverThread = new ServerThread(serverSocket);
        serverThread.start();

        socket = NativeSocket.openTcpIpv4Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", port));
    }

    private class ServerThread extends Thread {
        private ServerSocket serverSocket;

        public ServerThread(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        public void run() {
            try {
                serverSocket.accept();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
