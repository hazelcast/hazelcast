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

package com.hazelcast.impl;

import org.junit.Test;

import java.io.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class MapOperationStatsImplTest {

    @Test
    public void noOperation() throws Exception {
        MapOperationStatsImpl mapOperationStats = new MapOperationStatsImpl(100);
        Thread.sleep(10);
        MapOperationStats stats = mapOperationStats.getPublishedStats();
        assertThat(stats.getNumberOfGets(), equalTo((long) 0));
        assertThat(stats.getNumberOfPuts(), equalTo((long) 0));
        assertThat(stats.getNumberOfRemoves(), equalTo((long) 0));
        assertTrue(stats.getPeriodEnd() - stats.getPeriodStart() > 0);
    }

    @Test
    public void callGetPublishedStatsTwice() {
        MapOperationStatsImpl mapOperationStats = new MapOperationStatsImpl(100);
        MapOperationStats stats1 = mapOperationStats.getPublishedStats();
        MapOperationStats stats2 = mapOperationStats.getPublishedStats();
        assertTrue(stats1 == stats2);
    }

    @Test
    public void doAllOperations() {
        MapOperationStatsImpl mapOperationStats = new MapOperationStatsImpl(100);
        for (int i = 0; i < 10; i++) {
            mapOperationStats.incrementPuts();
            mapOperationStats.incrementGets();
            mapOperationStats.incrementRemoves();
        }
        assertEquals(10, mapOperationStats.getPublishedStats().getNumberOfGets());
        assertEquals(10, mapOperationStats.getPublishedStats().getNumberOfPuts());
        assertEquals(10, mapOperationStats.getPublishedStats().getNumberOfRemoves());
    }

    @Test
    public void testTotal() {
        MapOperationStatsImpl mapOperationStats = new MapOperationStatsImpl(100);
        for (int i = 0; i < 10; i++) {
            mapOperationStats.incrementPuts();
            mapOperationStats.incrementGets();
            mapOperationStats.incrementRemoves();
        }
        assertEquals(10, mapOperationStats.getPublishedStats().getNumberOfGets());
        assertEquals(10, mapOperationStats.getPublishedStats().getNumberOfPuts());
        assertEquals(10, mapOperationStats.getPublishedStats().getNumberOfRemoves());
        assertEquals(30, mapOperationStats.getPublishedStats().total());
    }

    @Test
    public void putInLessThanSubInterval() throws InterruptedException {
        MapOperationStatsImpl mapOperationStats = new MapOperationStatsImpl(100);
        long start = System.currentTimeMillis();
        long counter = 0;
        boolean run = true;
        while (run) {
            mapOperationStats.incrementPuts();
            counter++;
            Thread.sleep(1);
            if (System.currentTimeMillis() - start > 5) {
                run = false;
            }
        }
        MapOperationStats stats = mapOperationStats.getPublishedStats();
        assertEquals(counter, stats.getNumberOfPuts());
        long interval = stats.getPeriodEnd() - stats.getPeriodStart();
        assertTrue(interval < 50);
    }

    @Test
    public void putInHalfOfInterval() throws InterruptedException {
        MapOperationStatsImpl mapOperationStats = new MapOperationStatsImpl(100);
        long start = System.currentTimeMillis();
        long counter = 0;
        boolean run = true;
        while (run) {
            counter++;
            mapOperationStats.incrementPuts();
            if (System.currentTimeMillis() - start > 50) {
                run = false;
            }
            Thread.sleep(1);
        }
        mapOperationStats.incrementPuts();
        counter++;
        MapOperationStats stats = mapOperationStats.getPublishedStats();
        long interval = stats.getPeriodEnd() - stats.getPeriodStart();
        double statTps = stats.getNumberOfPuts() / interval;
        double totalTps = counter / (System.currentTimeMillis() - start);
        assertTrue(statTps < totalTps + 5);
        assertTrue(statTps > totalTps - 5);
        assertTrue(interval < 100);
    }

    @Test
    public void putLittleLessThanInterval() throws InterruptedException {
        MapOperationStatsImpl mapOperationStats = new MapOperationStatsImpl(100);
        long start = System.currentTimeMillis();
        long counter = 0;
        boolean run = true;
        while (run) {
            counter++;
            mapOperationStats.incrementPuts();
            if (System.currentTimeMillis() - start > 95) {
                run = false;
            }
            Thread.sleep(1);
        }
        mapOperationStats.incrementPuts();
        counter++;
        MapOperationStats stats = mapOperationStats.getPublishedStats();
        long interval = stats.getPeriodEnd() - stats.getPeriodStart();
        double statTps = stats.getNumberOfPuts() / interval;
        double totalTps = counter / (System.currentTimeMillis() - start);
        assertTrue(statTps < totalTps + 5);
        assertTrue(statTps > totalTps - 5);
//        assertTrue(interval < 99);
    }

    @Test
    public void putLittleMoreThanInterval() throws InterruptedException {
        MapOperationStatsImpl mapOperationStats = new MapOperationStatsImpl(100);
        long start = System.currentTimeMillis();
        long counter = 0;
        boolean run = true;
        while (run) {
            counter++;
            mapOperationStats.incrementPuts();
            if (System.currentTimeMillis() - start > 105) {
                run = false;
            }
            Thread.sleep(1);
        }
        mapOperationStats.incrementPuts();
        counter++;
        MapOperationStats stats = mapOperationStats.getPublishedStats();
        long interval = stats.getPeriodEnd() - stats.getPeriodStart();
        double statTps = stats.getNumberOfPuts() / interval;
        double totalTps = counter / (System.currentTimeMillis() - start);
        assertTrue(statTps < totalTps + 5);
        assertTrue(statTps > totalTps - 5);
//        assertEquals(100, interval);
    }

    @Test
    public void putWayMoreThanInterval() throws InterruptedException {
        MapOperationStatsImpl mapOperationStats = new MapOperationStatsImpl(100);
        long start = System.currentTimeMillis();
        long counter = 0;
        boolean run = true;
        while (run) {
            counter++;
            mapOperationStats.incrementPuts();
            if (System.currentTimeMillis() - start > 205) {
                run = false;
            }
            Thread.sleep(1);
        }
        mapOperationStats.incrementPuts();
        counter++;
        MapOperationStats stats = mapOperationStats.getPublishedStats();
        long interval = stats.getPeriodEnd() - stats.getPeriodStart();
        double statTps = stats.getNumberOfPuts() / interval;
        double totalTps = counter / (System.currentTimeMillis() - start);
        assertTrue(statTps < totalTps + 5);
        assertTrue(statTps > totalTps - 5);
//        assertEquals(100, interval);
    }

    @Test
    public void testToString() {
        MapOperationStatsImpl mapOperationStats = new MapOperationStatsImpl(100);
        String str = mapOperationStats.toString();
    }

    @Test
    public void testDataSerializable() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bos);
        MapOperationStatsImpl mapOperationStats = new MapOperationStatsImpl(100);
        mapOperationStats.incrementPuts();
        mapOperationStats.incrementGets();
        mapOperationStats.incrementRemoves();
        mapOperationStats.writeData(dout);
        MapOperationStatsImpl newStat = new MapOperationStatsImpl();
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());

        newStat.readData(new DataInputStream(bis));
        assertEquals(mapOperationStats.getNumberOfGets(), newStat.getNumberOfGets());
        assertEquals(mapOperationStats.getNumberOfPuts(), newStat.getNumberOfPuts());
        assertEquals(mapOperationStats.getNumberOfRemoves(), newStat.getNumberOfRemoves());
    }
}
