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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

        String commandLine = "MPUTALL default #"+ map.size()*2;
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
        out.write("\r\n".getBytes());
        out.flush();
        assertTrue(read(socket).contains("OK"));
        doOp("MENTRYSET map default", null);
        List<String> keys = read(socket);
        System.out.println("keys = " + keys);
    }

    @Test
    public void putIfAbsent() throws IOException {
        String key = "1";
        String value = "a";
               
        OutputStream out = doOp("MPUTIFABSENT default #2", "1 1");
        out.write(key.getBytes());
        out.write(value.getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        read(socket);
    }

    @Test
    public void replaceIfNotNull() throws IOException {
        put(socket, "1".getBytes(), "b".getBytes(), 0);
        String key = "1";
        String value = "a";

        OutputStream out = doOp("1 MREPLACEIFNOTNULL 1 default #2", "1 1");
        out.write(key.getBytes());
        out.write(value.getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        read(socket);
    }

    @Test
    public void removeIfSame() throws IOException {
        put(socket, "1".getBytes(), "a".getBytes(), 0);
        String key = "1";
        String value = "a";

        OutputStream out = doOp("MREMOVEIFSAME default #2", "1 1");
        out.write(key.getBytes());
        out.write(value.getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        assertTrue(read(socket).contains("true"));
    }

    @Test
    public void tryRemove() throws IOException {
        String key = "1";
        put(socket, key.getBytes(), "a".getBytes(), 0);

        OutputStream out = doOp("MTRYREMOVE default 10 #1", "1");
        out.write(key.getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        assertTrue(read(socket).contains("a"));
    }


    @Test
    public void replaceIfSame() throws IOException {
        put(socket, "1".getBytes(), "a".getBytes(), 0);
        String key = "1";
        String value = "a";

        OutputStream out = doOp("MREPLACEIFSAME default #3", "1 1 1");
        out.write(key.getBytes());
        out.write(value.getBytes());
        out.write("b".getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        assertTrue(read(socket).contains("true"));
    }

    @Test
    public void getAll() throws IOException {
        put(socket, "1".getBytes(), "a".getBytes(), 0);
        put(socket, "2".getBytes(), "b".getBytes(), 0);
        put(socket, "3".getBytes(), "c".getBytes(), 0);
        put(socket, "4".getBytes(), "d".getBytes(), 0);

        OutputStream out = doOp("MGETALL default 0 #3", "1 1 1");
        out.write("1".getBytes());
        out.write("2".getBytes());
        out.write("4".getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        List<String> list = read(socket);
        assertTrue(list.contains("a"));
        assertTrue(list.contains("b"));
        assertTrue(list.contains("d"));
        assertFalse(list.contains("c"));
    }

    @Test
    public void tryLock() throws IOException {
        OutputStream out = doOp("MTRYLOCK default 0 #1", ""+"1".getBytes().length);
        out.write("1".getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        assertTrue(read(socket).contains("true"));
    }

    @Test
    public void tryLockNGet() throws IOException {
        put(socket, "1".getBytes(), "a".getBytes(), 0);
        OutputStream out = doOp("MTRYLOCKANDGET default 0 #1", ""+"1".getBytes().length);
        out.write("1".getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        assertTrue(read(socket).contains("a"));
    }

    @Test
    public void lockNunlockKey() throws IOException, InterruptedException {
        OutputStream out = doOp("MLOCK default #1", ""+"1".getBytes().length);
        out.write("1".getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        assertTrue(read(socket).contains("OK"));
        boolean shouldFail = false;
        try{
            putFromAnotherThread("1", "b", 1000);
            assertFalse(true);
        }catch (RuntimeException e){
            shouldFail = true;
        }

        assertTrue(shouldFail);

        out = doOp("MUNLOCK default #1", ""+"1".getBytes().length);
        out.write("1".getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        assertTrue(read(socket).contains("OK"));
        try{
            putFromAnotherThread("1", "b", 1000);
            assertTrue(true);
        }catch (RuntimeException e){
            assertFalse(true);
        }

    }


    @Test
    public void lockNunlockKeyWithWrongThreadId() throws IOException, InterruptedException {
        OutputStream out = doOp("1 MLOCK 0 default #1", ""+"1".getBytes().length);
        out.write("1".getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        assertTrue(read(socket).contains("OK"));
        boolean shouldFail = false;
        try{
            putFromAnotherThread("1", "b", 1000);
            assertFalse(true);
        }catch (RuntimeException e){
            shouldFail = true;
        }

        assertTrue(shouldFail);

        out = doOp("2 MUNLOCK 0 default #1", ""+"1".getBytes().length);
        out.write("1".getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        assertFalse(read(socket).contains("OK"));
        try{
            putFromAnotherThread("1", "b", 1000);
            assertFalse(true);
        }catch (RuntimeException e){
            assertFalse(false);
        }

    }



    @Test
    public void getMapEntry() throws IOException {
        put(socket, "1".getBytes(), "a".getBytes(), 0);
        OutputStream out = doOp("MGETENTRY default #1", ""+"1".getBytes().length);
        out.write("1".getBytes());
        List<String> entry = read(socket);
        assertTrue(entry.contains("a"));

    }

    @Test
    public void keySet() throws IOException, InterruptedException {
        put(socket, "1".getBytes(), "a".getBytes(), 0);
        put(socket, "2".getBytes(), "b".getBytes(), 0);
        doOp("KEYSET map default", null);
        List<String> keys = read(socket);
        assertEquals(2, keys.size());
        assertTrue(keys.contains("1"));
        assertTrue(keys.contains("2"));
    }

    @Test
    public void entrySet() throws IOException, InterruptedException {
        put(socket, "1".getBytes(), "a".getBytes(), 0);
        put(socket, "2".getBytes(), "b".getBytes(), 0);
        doOp("MENTRYSET map default", null);
        List<String> keys = read(socket);
        assertEquals(4, keys.size());
        assertTrue(keys.contains("1"));
        assertTrue(keys.contains("2"));
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
    }

    @Test
    public void containsKey() throws IOException {
        put(socket, "1".getBytes(), "a".getBytes(), 0);
        put(socket, "2".getBytes(), "b".getBytes(), 0);
        OutputStream out = doOp("MCONTAINSKEY default #1", ""+"1".getBytes().length);
        out.write("1".getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        List<String> entry = read(socket);
        assertTrue(entry.contains("true"));
    }

    @Test
    public void containsValue() throws IOException {
        put(socket, "1".getBytes(), "a".getBytes(), 0);
        put(socket, "2".getBytes(), "b".getBytes(), 0);
        OutputStream out = doOp("MCONTAINSVALUE default #1", ""+"a".getBytes().length);
        out.write("a".getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        List<String> entry = read(socket);
        assertTrue(entry.contains("true"));
    }

    @Test
    public void addListenerReturnValueTrue() throws IOException, InterruptedException {
        doOp("MADDLISTENER default true", null);
        assertTrue(read(socket).contains("OK"));
        final String value = "a";
        putFromAnotherThread("1", value);
        List<String> values = read(socket);
        System.out.println("RESULT is " + values.get(0).equals(value));
        assertTrue(values.contains(value));
    }

    @Test
    public void addListenerReturnValueFalse() throws IOException, InterruptedException {
        doOp("MADDLISTENER default false", null);
        assertTrue(read(socket).contains("OK"));
        final String value = "a";
        putFromAnotherThread("1", value);
        List<String> values = read(socket);
        System.out.println("RESULT is " + values.get(0).equals(value));
        assertFalse(values.contains(value));
    }

    @Test
    public void addListenerToKey() throws IOException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable(){
            public void run() {
                try {
                    OutputStream out = doOp("MADDLISTENER default true #1", "" + "2".getBytes().length);
                    out.write("b".getBytes());
                    out.write("\r\n".getBytes());
                    out.flush();
                    assertTrue(read(socket).contains("OK"));
                    final String value = "a";
                    putFromAnotherThread("1", value);
                    List<String> values = read(socket);
                    latch.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        
        assertFalse(latch.await(20000, TimeUnit.MILLISECONDS));
        
        
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
        addListenerReturnValueTrue();
        doOp("REMOVELISTENER map default", null);
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
        OutputStream out = doOp("MPUT default " + ttl + " #2", "" + key.length + " " + value.length, socket);
        out.write(key);
        out.write(value);
        out.write("\r\n".getBytes());
        out.flush();
        return read(socket);
    }

    private List<String> get(Socket socket, byte[] keyInByte) throws IOException {
        OutputStream out = doOp("MGET default #1", "" + keyInByte.length);
        out.write(keyInByte);
        out.write("\r\n".getBytes());
        out.flush();
        return read(socket);
    }
}
