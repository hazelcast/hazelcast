/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.protocol;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class QueueTest extends ProtocolTest {

    @Test
    public void offer() throws IOException {
        String item = "1";
        assertTrue(offer(item, socket).contains("OK"));
    }

    private List<String> offer(String item, Socket socket) throws IOException {
        OutputStream out = doOp("QOFFER default 0 #1", "" + item.getBytes().length, socket);
        out.write(item.getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        return read(socket);
    }

    @Test
    public void poll() throws IOException {
        String item = "1";
        offer(item, socket);
        OutputStream out = doOp("QPOLL default 0", null, socket);
        out.flush();
        assertTrue(read(socket).contains(item));
    }

    @Test
    public void take() throws IOException {
        String item = "1";
        offer(item, socket);
        OutputStream out = doOp("QTAKE default", null, socket);
        out.flush();
        assertTrue(read(socket).contains(item));
    }

    @Test
    public void put() throws IOException {
        String item = "1";
        offer(item, socket);
        OutputStream out = doOp("QPUT default #1", "" + item.getBytes().length, socket);
        out.write(item.getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        assertTrue(read(socket).contains("true"));
    }

    @Test
    public void addListener() throws IOException {
        doOp("QLISTEN default true", null);
        assertTrue(read(socket).contains("OK"));
        final String item = "a";
        new Thread(new Runnable() {
            public void run() {
                try {
                    Socket socket = connect0();
                    offer(item, socket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        List<String> values = read(socket);
        assertTrue(values.contains(item));
    }
}
