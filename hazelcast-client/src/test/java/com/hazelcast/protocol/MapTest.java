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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapTest extends ProtocolTest{

    //HANDLE THE WRONG COMMANDS PROPERLY!

    @Test
    public void putAll() throws IOException {
        Map<String, String> map = new HashMap<String, String>();
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");
        map.put("4", "d");

        String commandLine = "MPUTALL 0 default #"+ map.size()*2;
        StringBuilder sizeLine = new StringBuilder("");

        int i = map.keySet().size();
        
        for(String k: map.keySet()){
            sizeLine.append(k.getBytes().length);
            sizeLine.append(" ").append(map.get(k).getBytes().length);
            i--;
            if(i != 0){
                sizeLine.append(" ");
            }
                    
        }
        OutputStream out = doOp(commandLine, sizeLine.toString());

        for(String k: map.keySet()){
            out.write(k.getBytes());
            out.write(map.get(k).getBytes());
        }
        assertTrue(read(socket).contains("OK"));
        doOp("ENTRYSET 0 map default", null);
        List<String> keys = read(socket);
        System.out.println("keys = " + keys);
    }

    @Test
    public void lockNunlockKey() throws IOException, InterruptedException {
        OutputStream out = doOp("MLOCK 0 default 0 #1", ""+"1".getBytes().length);
        out.write("1".getBytes());
        assertTrue(read(socket).contains("OK"));
        boolean shouldFail = false;
        try{
            putFromAnotherThread("1", "b", 1000);
            assertFalse(true);
        }catch (RuntimeException e){
            shouldFail = true;
        }

        assertTrue(shouldFail);

        out = doOp("MUNLOCK 0 default #1", ""+"1".getBytes().length);
        out.write("1".getBytes());
        assertTrue(read(socket).contains("OK"));
        try{
            putFromAnotherThread("1", "b", 1000);
            assertTrue(true);
        }catch (RuntimeException e){
            assertFalse(true);
        }

    }

    @Test
    public void getMapEntry() throws IOException {
        put(socket, "1".getBytes(), "a".getBytes(), 0);
        OutputStream out = doOp("MGETENTRY 0 default #1", ""+"1".getBytes().length);
        out.write("1".getBytes());
        List<String> entry = read(socket);
        assertTrue(entry.contains("a"));

    }

    @Test
    public void keySet() throws IOException, InterruptedException {
        put(socket, "1".getBytes(), "a".getBytes(), 0);
        put(socket, "2".getBytes(), "b".getBytes(), 0);
        doOp("KEYSET 0 map default", null);
        List<String> keys = read(socket);
        assertEquals(2, keys.size());
        assertTrue(keys.contains("1"));
        assertTrue(keys.contains("2"));
    }

    @Test
    public void entrySet() throws IOException, InterruptedException {
        put(socket, "1".getBytes(), "a".getBytes(), 0);
        put(socket, "2".getBytes(), "b".getBytes(), 0);
        doOp("ENTRYSET 0 map default", null);
        List<String> keys = read(socket);
        assertEquals(4, keys.size());
        assertTrue(keys.contains("1"));
        assertTrue(keys.contains("2"));
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
    }
    @Test
    public void addListener() throws IOException, InterruptedException {
        doOp("ADDLISTENER 3 map default true", null);
        assertTrue(read(socket).contains("OK"));
        final String value = "a";
        putFromAnotherThread("1", value);
        List<String> values = read(socket);
        System.out.println("RESULT is " + values.get(0).equals(value));
        assertTrue(values.contains(value));
    }

    protected void putFromAnotherThread(final String key, final String value) throws InterruptedException {
        putFromAnotherThread(key, value, 0);
    }

    protected void putFromAnotherThread(final String key, final String value, int timeout) throws InterruptedException {
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    Socket socket = connect0();
                    put(socket, key.getBytes(), value.getBytes(), 0);
                    socket.close();
                } catch (IOException e) {
                }
            }
        });
        t.start();
        long start = System.currentTimeMillis();
        t.join(timeout);
        long interval = System.currentTimeMillis() - start;
        System.out.println(interval + ":: " + timeout);
        if(timeout != 0 && interval > timeout-100){
            throw new RuntimeException("Timeouted");
        }
    }

    @Test
    public void removeListener() throws IOException, InterruptedException {
        addListener();
        doOp("REMOVELISTENER 5 map default", null);
        assertTrue(read(socket).contains("OK"));
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

    @Test
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
        OutputStream out = doOp("MPUT 1 default " + ttl + " #2", "" + key.length + " " + value.length, socket);
        out.write(key);
        out.write(value);
        out.flush();
        return read(socket);
    }

    private List<String> get(Socket socket, byte[] keyInByte) throws IOException {
        OutputStream out = doOp("MGET 2 default #1", "" + keyInByte.length);
        out.write(keyInByte);
        out.flush();
        return read(socket);
    }
}
