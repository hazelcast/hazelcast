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

package com.hazelcast.test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.CopyOnWriteArrayList;

public class Parallel {

    static final SimpleDateFormat format = new SimpleDateFormat("dd HH:mm:ss");

    public static void main(String[] args) {
        final String concurrencyLevelStr = (args == null || args.length <= 0) ? "1" : args[0];
        final String profile = (args == null || args.length <= 1) ? "all" : args[1];
        final int concurrencyLevel = Integer.parseInt(concurrencyLevelStr);
        final Thread[] threads = new Thread[concurrencyLevel];
        System.out.println(format.format(new Date()) + " PARALLEL TESTS STARTED concurrency:"
                + concurrencyLevel + " profile:" + profile);
        long start = System.currentTimeMillis();
        final Collection<Process> colProcesses = new CopyOnWriteArrayList<Process>();
        for (int i = 0; i < concurrencyLevel; i++) {
            final String index = Integer.toString(i);
            Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        long processStart = System.currentTimeMillis();
                        //mvn -Dhazelcast.test.index=0 -Dhazelcast.test.concurrency.level=2 -P all test -DreportNameSuffix=0
                        String[] exec = new String[]{"mvn", "-Dhazelcast.test.index=" + index,
                                "-Dhazelcast.test.concurrency.level=" + concurrencyLevel,
                                "-P", profile, "test",
                                "-DreportNameSuffix=" + index};
                        ProcessBuilder processBuilder = new ProcessBuilder(exec);
                        processBuilder.redirectErrorStream(true);
                        Process proc = processBuilder.start();
                        colProcesses.add(proc);
                        InputStream in = proc.getInputStream();
                        BufferedReader br = new BufferedReader(new InputStreamReader(in));
                        String str;
                        while ((str = br.readLine()) != null) {
                            if (str.contains("Started") || str.contains("Finished") || str.startsWith("PLOG:")) {
                                System.out.println("[" + index + "] " + str);
                            } else if (str.startsWith("[ERROR]") || str.startsWith("[WARNING]")) {
                                System.err.println("[" + index + "] " + str);
                            }
                        }
                        try {
                            proc.waitFor();
                        } catch (InterruptedException e) {
                            System.err.println("[" + index + "] Process was interrupted");
                        }
                        System.out.println(proc.exitValue());
                        br.close();
                        long now = System.currentTimeMillis();
                        long seconds = (now - processStart) / 1000;
                        long minutes = seconds / 60;
                        long remainingSeconds = seconds % 60;
                        System.out.println("-------------------------------------------------");
                        System.out.println();
                        System.out.println(format.format(new Date()) + ": DONE!! [" + index + "] in " + minutes + " min. and "
                                + remainingSeconds + " secs.");
                        System.out.println();
                        System.out.println("-------------------------------------------------");
                        proc.destroy();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            threads[i] = t;
            t.start();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                for (Process process : colProcesses) {
                    try {
                        System.out.println("Destroying process");
                        process.destroy();
                    } catch (Exception ignored) {
                    }
                }
            }
        }));
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long now = System.currentTimeMillis();
        long seconds = (now - start) / 1000;
        long minutes = seconds / 60;
        long remainingSeconds = seconds % 60;
        System.out.println("=================================================");
        System.out.println();
        System.out.println(format.format(new Date()) + ": Completed.");
        System.out.println("TOOK : " + minutes + " minutes and " + remainingSeconds + " secs.");
        System.out.println();
        System.out.println("=================================================");
    }
}
