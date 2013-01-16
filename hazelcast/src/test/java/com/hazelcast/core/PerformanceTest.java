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

package com.hazelcast.core;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.*;
import java.util.Map;

public class PerformanceTest {
    private static final boolean createBaseline = true;
    private static final String outputFile = "C:\\Temp\\hazelcast_out.csv";
    private static final String inputFileName = "C:\\Temp\\hazelcast_in.csv";
    private static IMap<String, PerformanceTimer> baselinePerformance;
    private static IMap<String, PerformanceTimer> performance;
    protected static long ops = 10000;
    protected static PerformanceTimer t;

    public PerformanceTest() {
    }

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
        baselinePerformance = Hazelcast.getMap("baselinePerformance");
        performance = Hazelcast.getMap("performance");
    }

    @AfterClass
    public static void writeResults() {
        try {
            writeBaseline();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readBaseline() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFileName));
        String line;
        while ((line = reader.readLine()) != null) {
            line.split(",");
        }
        reader.close();
    }

    public static void writeBaseline() throws IOException {
        BufferedWriter writer = new BufferedWriter((new FileWriter(outputFile)));
        for (Map.Entry<String, PerformanceTimer> measurement : baselinePerformance.entrySet()) {
            writer.write(measurement.getValue().toString());
            writer.newLine();
        }
        writer.flush();
        writer.close();
    }

    @After
    public void addMeasurement() {
        if (createBaseline) {
            baselinePerformance.put(t.getTestName(), t);
        } else {
            performance.put(t.getTestName(), t);
        }
    }
}
