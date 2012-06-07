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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class Main {

    Socket socket;

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.getCluster();
    }

    @After
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void connect() throws Exception {
        this.socket = connect0();
    }


    //HANDLE THE WRONG COMMANDS PROPERLY!

    private Socket connect0() throws IOException {
        final InetSocketAddress isa = new InetSocketAddress("localhost", 5701);
        Socket socket = new Socket();
        socket.setKeepAlive(true);
        socket.setSoLinger(true, 5);
        socket.connect(isa, 3000);
        OutputStream out = socket.getOutputStream();
        out.write("P01".getBytes());
        out.flush();
        auth(socket);
        return socket;
    }

    @Test
    public void entrySet() throws IOException {
        doOp("ENTRYSET 0 default", "#0");
        InputStreamReader reader = new InputStreamReader(socket.getInputStream(), "UTF-8");
        BufferedReader buf = new BufferedReader(reader);
        String commandLine = buf.readLine();
        String sizeLine = buf.readLine();
        System.out.println(commandLine);
        System.out.println(sizeLine);
        assertTrue(commandLine.startsWith("SUCCESS"));

    }

    @Test
    public void addListener() throws IOException, InterruptedException {
        doOp("ADDLSTNR 3 map default true", "#0");
        assertTrue(read(socket).contains("SUCCESS"));
        final String value = "a";
        putFromAnotherThread("1", value);
        List<String> values = read(socket);
        System.out.println("RESULT is " + values.get(0).equals(value));
        assertTrue(values.contains(value));
    }


    private OutputStream doOp(String commandLine, String sizeLine) throws IOException {
        return doOp(commandLine, sizeLine, socket);
    }

    private OutputStream doOp(String commandLine, String sizeLine, Socket socket) throws IOException {
        OutputStream out = socket.getOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");
        writer.write(commandLine);
        writer.write("\r\n");
        writer.write(sizeLine);
        writer.write("\r\n");
        writer.flush();
        return out;
    }

    private void putFromAnotherThread(final String key, final String value) throws InterruptedException {
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    Socket socket = connect0();
                    put(socket, key.getBytes(), value.getBytes(), 0);
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
        t.join();
    }

    @Test
    public void removeListener() throws IOException, InterruptedException {
        addListener();
        doOp("RMVLSTNR 5 map default", "#0");
        assertTrue(read(socket).contains("SUCCESS"));
        putFromAnotherThread("1", "b");
        Thread.sleep(1000);
        assertTrue(socket.getInputStream().available() <= 0);
    }

    @Test
    public void put() throws IOException {
        put(socket, "1".getBytes("UTF-8"), "istanbul".getBytes("UTF-8"), 0);
    }

    @Test
    public void get() throws IOException {
        put(socket, "1".getBytes("UTF-8"), "istanbul".getBytes("UTF-8"), 0);
        List<String> values = get(socket, "1".getBytes("UTF-8"));
        assertTrue(values.contains("istanbul"));
    }

    public void perf() throws IOException, InterruptedException {
        put(socket, "1".getBytes("UTF-8"), "istanbul".getBytes("UTF-8"), 0);
        Thread.sleep(2000);
        final AtomicInteger c = new AtomicInteger(0);
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                int current = c.getAndSet(0);
                System.out.println(current + " ops per second");
            }
        }, 1000, 1000);
        for (; ; ) {
            put(socket, "1".getBytes("UTF-8"), "istanbul".getBytes("UTF-8"), 0l);
            c.incrementAndGet();
        }
    }

    private List<String> put(Socket socket, byte[] key, byte[] value, long ttl) throws IOException {
        OutputStream out = doOp("MPUT 1 default " + ttl, "#2 " + key.length + " " + value.length);
        out.write(key);
        out.write(value);
        out.flush();
        return read(socket);
    }

    private List<String> get(Socket socket, byte[] keyInByte) throws IOException {
        OutputStream out = doOp("MGET 2 default", "#1 " + keyInByte.length);
        out.write(keyInByte);
        out.flush();
        return read(socket);
    }

    private void auth(Socket socket) throws IOException {
        doOp("AUTH 0 dev dev-pass", "#0", socket);
        read(socket);
    }

    private List<String> read(Socket socket) throws IOException {
        List<String> values = new ArrayList<String>();
        InputStreamReader reader = new InputStreamReader(socket.getInputStream(), "UTF-8");
        BufferedReader buf = new BufferedReader(reader);
        String commandLine = buf.readLine();
        String sizeLine = buf.readLine();
        System.out.println(commandLine);
        System.out.println(sizeLine);
        String[] tokens = sizeLine.split(" ");
        int count = Integer.valueOf(tokens[0].substring(1));
//        System.out.println("Count is " + count);
        for (int i = 0; i < count; i++) {
            int s = Integer.valueOf(tokens[i + 1]);
//            System.out.println("Size is " + s);
            char[] value = new char[s];
            buf.read(value);
            System.out.println(String.valueOf(value));
            values.add(String.valueOf(value));
        }
        if (count == 0) {
            values.add(commandLine.split(" ")[0]);
        }
        return values;
    }
}
