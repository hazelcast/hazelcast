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

package com.hazelcast.core;

import java.io.Serializable;

public class PerformanceTimer implements Serializable {

    private String testName;
    private long start;
    private long end;
    private long numberOfOperations;

    public PerformanceTimer() {
        this.start = System.currentTimeMillis();
    }

    public PerformanceTimer(long numberOfOperations) {
        this.start = System.currentTimeMillis();
        this.numberOfOperations = numberOfOperations;
    }

    public PerformanceTimer(String testName, long numberOfOperations) {
        this.start = System.currentTimeMillis();
        this.numberOfOperations = numberOfOperations;
        this.testName = testName;
    }

    public String getTestName() {
        return testName;
    }

    @Override
    public String toString() {
        StringBuilder m = new StringBuilder();
        m.append(testName);
        m.append(",");
        m.append(numberOfOperations);
        m.append(",");
        m.append(elapsed());
        m.append(",");
        m.append(operationsPerSecond());
        return m.toString();
    }

    public static PerformanceTimer fromString(String pm) {
        String[] entries = pm.split(",");
        PerformanceTimer perf = new PerformanceTimer(entries[0], Long.parseLong(entries[1]));
        perf.setElapsed(Long.parseLong(entries[2]));
        return perf;
    }

    public void stop() {
        this.end = System.currentTimeMillis();
    }

    public long elapsed() {
        return end - start;
    }

    private void setElapsed(long elapsed) {
        start = 0;
        end = elapsed;
    }

    public double operationsPerSecond() {
        return (double) numberOfOperations / ((double) elapsed() / 1000d);
    }

    public void printResult() {
        System.out.println(toString());
        /*System.out.println(testName);
        System.out.println("number of operations: " + numberOfOperations);
        System.out.println("elapsed (ms): " + elapsed());
        System.out.println("operations/sec: " + operationsPerSecond());*/
    }
}
