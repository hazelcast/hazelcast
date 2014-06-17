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

package com.hazelcast.management;

/**
 * Holds monitoring information( CPU percentage etc.) for threads.
 */
public class MonitoredThread implements Comparable<MonitoredThread> {

    final String name;
    final long threadId;
    final int cpuPercentage;

    public MonitoredThread(String name, long threadId, int cpuPercentage) {
        this.name = name;
        this.threadId = threadId;
        this.cpuPercentage = cpuPercentage;
    }

    @Override
    public String toString() {
        return "MonitoredThread{"
                + "name='" + name + '\''
                + ", threadId=" + threadId
                + ", cpuPercentage=" + cpuPercentage
                + '}';
    }

    public int compareTo(MonitoredThread o) {
        return name.compareTo(o.name);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o != null && o instanceof MonitoredThread) {
            return this.compareTo((MonitoredThread) o) == 0;
        }
        return false;
    }
}
