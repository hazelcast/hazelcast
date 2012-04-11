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

package com.hazelcast.impl;

import com.hazelcast.impl.monitor.LocalMapOperationStatsImpl;
import com.hazelcast.impl.monitor.MapOperationsCounter;
import com.hazelcast.monitor.LocalMapOperationStats;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapOperationsCounterTest {

    @Test
    public void noOperation() throws Exception {
        MapOperationsCounter mapOperationStats = new MapOperationsCounter(100);
        Thread.sleep(10);
        LocalMapOperationStats stats = mapOperationStats.getPublishedStats();
        assertThat(stats.getNumberOfGets(), equalTo((long) 0));
        assertThat(stats.getNumberOfPuts(), equalTo((long) 0));
        assertThat(stats.getNumberOfRemoves(), equalTo((long) 0));
        assertTrue(stats.getPeriodEnd() - stats.getPeriodStart() > 0);
    }

    @Test
    public void callGetPublishedStatsTwice() {
        MapOperationsCounter mapOperationStats = new MapOperationsCounter(100);
        LocalMapOperationStats stats1 = mapOperationStats.getPublishedStats();
        LocalMapOperationStats stats2 = mapOperationStats.getPublishedStats();
        assertTrue(stats1 == stats2);
    }

    @Test
    public void doAllOperations() {
        MapOperationsCounter mapOperationStats = new MapOperationsCounter(100);
        for (int i = 0; i < 10; i++) {
            mapOperationStats.incrementPuts(0);
            mapOperationStats.incrementGets(0);
            mapOperationStats.incrementRemoves(0);
        }
        assertEquals(10, mapOperationStats.getPublishedStats().getNumberOfGets());
        assertEquals(10, mapOperationStats.getPublishedStats().getNumberOfPuts());
        assertEquals(10, mapOperationStats.getPublishedStats().getNumberOfRemoves());
    }

    @Test
    public void testTotal() {
        MapOperationsCounter mapOperationStats = new MapOperationsCounter(100);
        for (int i = 0; i < 10; i++) {
            mapOperationStats.incrementPuts(0);
            mapOperationStats.incrementGets(0);
            mapOperationStats.incrementRemoves(0);
        }
        assertEquals(10, mapOperationStats.getPublishedStats().getNumberOfGets());
        assertEquals(10, mapOperationStats.getPublishedStats().getNumberOfPuts());
        assertEquals(10, mapOperationStats.getPublishedStats().getNumberOfRemoves());
        assertEquals(30, mapOperationStats.getPublishedStats().total());
    }

    @Test
    public void putInLessThanSubInterval() throws InterruptedException {
        MapOperationsCounter mapOperationStats = new MapOperationsCounter(100);
        long start = Clock.currentTimeMillis();
        long counter = 0;
        boolean run = true;
        while (run) {
            mapOperationStats.incrementPuts(0);
            counter++;
            Thread.sleep(1);
            if (Clock.currentTimeMillis() - start > 5) {
                run = false;
            }
        }
        LocalMapOperationStats stats = mapOperationStats.getPublishedStats();
        assertEquals(counter, stats.getNumberOfPuts());
        long interval = stats.getPeriodEnd() - stats.getPeriodStart();
    }

    @Test
    public void putInHalfOfInterval() throws InterruptedException {
        MapOperationsCounter mapOperationStats = new MapOperationsCounter(100);
        long start = Clock.currentTimeMillis();
        long counter = 0;
        boolean run = true;
        while (run) {
            counter++;
            mapOperationStats.incrementPuts(0);
            if (Clock.currentTimeMillis() - start > 50) {
                run = false;
            }
            Thread.sleep(1);
        }
        mapOperationStats.incrementPuts(0);
        counter++;
        LocalMapOperationStats stats = mapOperationStats.getPublishedStats();
        long interval = stats.getPeriodEnd() - stats.getPeriodStart();
        double statTps = stats.getNumberOfPuts() / interval;
        double totalTps = (double) counter / (Clock.currentTimeMillis() - start);
        assertTrue(statTps < totalTps + 5);
        assertTrue(statTps > totalTps - 5);
    }

    @Test
    public void putLittleLessThanInterval() throws InterruptedException {
        MapOperationsCounter mapOperationStats = new MapOperationsCounter(100);
        long start = Clock.currentTimeMillis();
        long counter = 0;
        boolean run = true;
        while (run) {
            counter++;
            mapOperationStats.incrementPuts(0);
            if (Clock.currentTimeMillis() - start > 95) {
                run = false;
            }
            Thread.sleep(1);
        }
        mapOperationStats.incrementPuts(0);
        counter++;
        LocalMapOperationStats stats = mapOperationStats.getPublishedStats();
        long interval = stats.getPeriodEnd() - stats.getPeriodStart();
        double statTps = stats.getNumberOfPuts() / interval;
        double totalTps = (double) counter / (Clock.currentTimeMillis() - start);
        assertTrue(statTps < totalTps + 5);
        assertTrue(statTps > totalTps - 5);
    }

    @Test
    public void putLittleMoreThanInterval() throws InterruptedException {
        MapOperationsCounter mapOperationStats = new MapOperationsCounter(100);
        long start = Clock.currentTimeMillis();
        long counter = 0;
        boolean run = true;
        while (run) {
            counter++;
            mapOperationStats.incrementPuts(0);
            if (Clock.currentTimeMillis() - start > 105) {
                run = false;
            }
            Thread.sleep(1);
        }
        mapOperationStats.incrementPuts(0);
        counter++;
        LocalMapOperationStats stats = mapOperationStats.getPublishedStats();
        long interval = stats.getPeriodEnd() - stats.getPeriodStart();
        double statTps = stats.getNumberOfPuts() / interval;
        double totalTps = (double) counter / (Clock.currentTimeMillis() - start);
        assertTrue(statTps < totalTps + 5);
        assertTrue(statTps > totalTps - 5);
    }

    @Test
    public void putWayMoreThanInterval() throws InterruptedException {
        MapOperationsCounter mapOperationStats = new MapOperationsCounter(100);
        long start = Clock.currentTimeMillis();
        long counter = 0;
        boolean run = true;
        while (run) {
            counter++;
            mapOperationStats.incrementPuts(0);
            if (Clock.currentTimeMillis() - start > 205) {
                run = false;
            }
            Thread.sleep(1);
        }
        mapOperationStats.incrementPuts(0);
        counter++;
        LocalMapOperationStats stats = mapOperationStats.getPublishedStats();
        long interval = stats.getPeriodEnd() - stats.getPeriodStart();
        double statTps = stats.getNumberOfPuts() / interval;
        double totalTps = (double) counter / (Clock.currentTimeMillis() - start);
        assertTrue(statTps < totalTps + 5);
        assertTrue(statTps > totalTps - 5);
    }

    @Test
    public void testDataSerializable() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bos);
        MapOperationsCounter mapOperationStats = new MapOperationsCounter(100);
        mapOperationStats.incrementPuts(0);
        mapOperationStats.incrementGets(0);
        mapOperationStats.incrementRemoves(0);
        ((DataSerializable) mapOperationStats.getPublishedStats()).writeData(dout);
        LocalMapOperationStatsImpl newStat = new LocalMapOperationStatsImpl();
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        newStat.readData(new DataInputStream(bis));
        assertEquals(mapOperationStats.getPublishedStats().getNumberOfGets(), newStat.getNumberOfGets());
        assertEquals(mapOperationStats.getPublishedStats().getNumberOfPuts(), newStat.getNumberOfPuts());
        assertEquals(mapOperationStats.getPublishedStats().getNumberOfRemoves(), newStat.getNumberOfRemoves());
        String str = newStat.toString();
    }
}
