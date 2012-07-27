/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.IMap;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class SimpleMapTest extends ProtocolTest{

    public static int THREAD_COUNT = 1;
    public static int ENTRY_COUNT = 10 * 1000;
    public static int VALUE_SIZE = 1000;
    public static final int STATS_SECONDS = 10;
    public static int GET_PERCENTAGE = 0;
    public static int PUT_PERCENTAGE = 100;
    final static Stats stats = new Stats();
    private static final byte[] NEWLINE = "\r\n".getBytes();

    public static void main(String[] args) {
        SimpleMapTest sm = new SimpleMapTest();
        sm.run(args);

    }

    OutputStream out = null;
    protected OutputStream doOp0(String commandLine, String sizeLine, Socket socket) throws IOException {
        if(out==null)
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 16 << 10));
        
        out.write(commandLine.getBytes());
        out.write("\r\n".getBytes());
        if (sizeLine != null) {
            out.write(sizeLine.getBytes());
            out.write(NEWLINE);
        }
        return out;
    }
    public static String[] fastSplit(String line, char split) {
        String[] temp = new String[line.length() / 2 + 1];
        int wordCount = 0;
        int i = 0;
        int j = line.indexOf(split);  // First substring
        while (j >= 0) {
            temp[wordCount++] = line.substring(i, j);
            i = j + 1;
            j = line.indexOf(split, i);   // Rest of substrings
        }
        temp[wordCount++] = line.substring(i); // Last substring
        String[] result = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);
        return result;
    }

    protected List<String> read(Socket socket) throws IOException {
        List<String> values = new ArrayList<String>();
        InputStreamReader reader = new InputStreamReader(socket.getInputStream(), "UTF-8");
        BufferedReader buf = new BufferedReader(reader);
        String commandLine = buf.readLine();
//        System.out.println("Reading " + commandLine);
        if(commandLine == null){
            return Collections.emptyList();
        }
        String[] split = fastSplit(commandLine, ' ');
        if (split[split.length - 1].startsWith("#")) {
            String sizeLine = buf.readLine();
            String[] tokens = fastSplit(sizeLine, ' ');
            int count = Integer.valueOf(split[split.length - 1].substring(1));
            for (int i = 0; i < count; i++) {
                int s = Integer.valueOf(tokens[i]);
//                System.out.println("Binary "+ i + ":: size is " + s);
                char[] value = new char[s];
                buf.read(value);
                values.add(String.valueOf(value));
            }
        }
//        if (values.size()  == 0) {
//            values.addAll(Arrays.asList(split));
//        }
        return values;
    }

    public void run(String[] args) {
        if (args != null && args.length > 0) {
            for (String arg : args) {
                arg = arg.trim();
                if (arg.startsWith("t")) {
                    THREAD_COUNT = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("c")) {
                    ENTRY_COUNT = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("v")) {
                    VALUE_SIZE = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("g")) {
                    GET_PERCENTAGE = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("p")) {
                    PUT_PERCENTAGE = Integer.parseInt(arg.substring(1));
                }
            }
        } else {
            System.out.println("Help: sh test.sh t200 v130 p10 g85 ");
            System.out.println("    // means 200 threads, value-size 130 bytes, 10% put, 85% get");
            System.out.println("");
        }
        System.out.println("Starting Test with ");
        System.out.println("      Thread Count: " + THREAD_COUNT);
        System.out.println("       Entry Count: " + ENTRY_COUNT);
        System.out.println("        Value Size: " + VALUE_SIZE);
        System.out.println("    Get Percentage: " + GET_PERCENTAGE);
        System.out.println("    Put Percentage: " + PUT_PERCENTAGE);
        System.out.println(" Remove Percentage: " + (100 - (PUT_PERCENTAGE + GET_PERCENTAGE)));
        ExecutorService es = Executors.newFixedThreadPool(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            es.submit(new Runnable() {
                public void run() {
                    try {
                        Socket socket  = connect0();
//                        long t = 10;
                        while (true) {

//                            String key = String.valueOf((int) (Math.random() * ENTRY_COUNT));
                            String key = "1";
//                            ByteBuffer bb = ByteBuffer.allocate(8);
//                            final long time = t++;
                            long time = System.nanoTime();
                            
//                            byte[] value = bb.putLong(time).array();
                            byte[] value = String.valueOf(time).getBytes();
//                            byte[] value = new byte[VALUE_SIZE];
//                            byte[] value = new byte[]{1,1,1,1,1,1,1,1};
//                            ArrayList<Byte> valueArray = new ArrayList<Byte>(value.length);
//                            for (byte b : value) {
//                                valueArray.add(b);
//                            }
//                            System.out.println(key + ":: RUNNININGINIGNIGNIN :: "  + time +  " value : " + valueArray);
                            int operation = ((int) (Math.random() * 100));
                            if (operation < GET_PERCENTAGE) {
                                OutputStream out = doOp0("MGET flag default #1", "" + key.length(), socket);
                                out.write(key.getBytes());
                                out.write(NEWLINE);
                                out.flush();
                                String response = read(socket).get(0);
                                stats.gets.incrementAndGet();
                            } else if (operation < GET_PERCENTAGE + PUT_PERCENTAGE) {
//                                System.out.println("Doing a put");
                                DataOutputStream out = (DataOutputStream) doOp0("MPUT flag default 0 #2", "" + key.length() + " " + value.length, socket);
                                out.write(key.getBytes());
                                out.write(value);
                                out.write(NEWLINE);
                                out.flush();
//                                String response = read(socket).get(0);
                                read(socket);
//                                System.out.println("Latency is" + (Integer.valueOf(response) - Integer.valueOf(new String(value))));
                                stats.puts.incrementAndGet();
                            } else {
                                OutputStream out = doOp0("MREMOVE flag default #1", "" + key.length(), socket);
                                out.write(key.getBytes());
                                out.write(NEWLINE);
                                out.flush();
                                String response = read(socket).get(0);
                                stats.removes.incrementAndGet();
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            });
        }
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        Stats statsNow = stats.getAndReset();
                        System.out.println(statsNow);
                        System.out.println("Operations per Second : " + statsNow.total() / STATS_SECONDS);
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        });
    }

    public static class Stats {
        public AtomicLong gets = new AtomicLong();
        public AtomicLong puts = new AtomicLong();
        public AtomicLong removes = new AtomicLong();

        public Stats getAndReset() {
            long getsNow = gets.getAndSet(0);
            long putsNow = puts.getAndSet(0);
            long removesNow = removes.getAndSet(0);
            Stats newOne = new Stats();
            newOne.gets.set(getsNow);
            newOne.puts.set(putsNow);
            newOne.removes.set(removesNow);
            return newOne;
        }

        public long total() {
            return gets.get() + puts.get() + removes.get();
        }

        public String toString() {
            return "total= " + total() + ", gets:" + gets.get() + ", puts:" + puts.get() + ", removes:" + removes.get();
        }
    }
}