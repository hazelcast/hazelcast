/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.protocol;

import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import static org.junit.Assert.assertTrue;

public class TopicTest extends ProtocolTest {

    @Test
    public void publish() throws IOException {
        String message = "m";
        OutputStream out = doOp("TPUBLISH flag default #1", "" + message.getBytes().length);
        out.write(message.getBytes());
        out.write("\r\n".getBytes());
        out.flush();
    }

    @Test
    public void addListener() throws IOException {
        OutputStream out = doOp("TADDLISTENER flag default", null, socket);
        out.flush();
        assertTrue(read(socket).contains("OK"));
        final String message = "m";
        new Thread(new Runnable() {
            public void run() {
                try {
                    Socket socket = connect0();
                    OutputStream out = doOp("TPUBLISH flag default #1", "" + message.getBytes().length, socket);
                    out.write(message.getBytes());
                    out.write("\r\n".getBytes());
                    out.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        assertTrue(read(socket).contains(message));
    }
}
