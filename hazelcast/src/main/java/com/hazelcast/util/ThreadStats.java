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

public class ThreadStats {
    private final long waitCount;
    private final long runCount;
    private final int utilizationPercentage;
    private final long createMillis;
    private final boolean running;

    public ThreadStats(int utilizationPercentage, long runCount, long waitCount, boolean running) {
        this.utilizationPercentage = utilizationPercentage;
        this.runCount = runCount;
        this.waitCount = waitCount;
        this.running = running;
        this.createMillis = Clock.currentTimeMillis();
    }

    public long getRunCount() {
        return runCount;
    }

    public int getUtilizationPercentage() {
        return utilizationPercentage;
    }

    public long getWaitCount() {
        return waitCount;
    }

    public long getCreateMillis() {
        return createMillis;
    }

    public long getSecondsBetween(long millis) {
        return (millis - createMillis) / 1000;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public String toString() {
        return "ThreadStats{" +
                "utilization=" + utilizationPercentage +
                ", runs=" + runCount +
                ", waits=" + waitCount +
                ", running=" + running +
                '}';
    }
}
