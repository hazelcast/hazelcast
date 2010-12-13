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

package com.hazelcast.util;

public class ThreadStats {
    private final long waitCount;
    private final long runCount;
    private final int utilizationPercentage;

    public ThreadStats(int utilizationPercentage, long runCount, long waitCount) {
        this.utilizationPercentage = utilizationPercentage;
        this.runCount = runCount;
        this.waitCount = waitCount;
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

    @Override
    public String toString() {
        return "ThreadStats{" +
                "utilization=" + utilizationPercentage +
                ", runCount=" + runCount +
                ", waitCount=" + waitCount +
                '}';
    }
}
