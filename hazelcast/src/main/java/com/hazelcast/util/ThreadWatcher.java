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

package com.hazelcast.util;

public class ThreadWatcher {
    long start = 0;
    long totalWait = 0;
    long totalTime = 0;
    long waitCount = 0;
    long runCount = 0;

    public void addWait(long totalWaitNanos, long now) {
        if (start == 0) {
            start = now;
        } else {
            totalWait += totalWaitNanos;
            totalTime += (now - start);
            start = now;
        }
        waitCount++;
    }

    public long incrementRunCount() {
        long now = System.nanoTime();
        if (start == 0) {
            start = now;
        } else {
            totalTime += (now - start);
            start = now;
        }
        return runCount++;
    }

    public ThreadStats publish(boolean running) {
        long now = System.nanoTime();
        totalTime += (now - start);
        long totalRun = totalTime - totalWait;
        int utilizationPercentage = (totalTime <= 0 || runCount <= 1) ? 0 : (int) ((totalRun * 100L) / totalTime);
        ThreadStats threadStats = new ThreadStats(utilizationPercentage, runCount, waitCount, running);
        start = now;
        totalTime = 0;
        totalWait = 0;
        return threadStats;
    }
}
