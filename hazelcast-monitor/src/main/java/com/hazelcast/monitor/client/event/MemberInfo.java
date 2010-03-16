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

package com.hazelcast.monitor.client.event;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MemberInfo implements ChangeEvent, Serializable {
    private int clusterID;
    private Date date;
    private Set<Integer> partitions;
    private long time;
    private long totalMemory;
    private long freeMemory;
    private long maxMemory;
    private int availableProcessors;
    private HashMap systemProps = new HashMap();

    public MemberInfo() {
    }

    public MemberInfo(int clusterID) {
        this.clusterID = clusterID;
        this.date = new Date();
    }

    public ChangeEventType getChangeEventType() {
        return ChangeEventType.MEMBER_INFO;
    }

    public int getClusterId() {
        return clusterID;
    }

    public Date getCreatedDate() {
        return date;
    }

    public long getTime() {
        return time;
    }

    public long getTotalMemory() {
        return totalMemory;
    }

    public long getFreeMemory() {
        return freeMemory;
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public HashMap getSystemProps() {
        return systemProps;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public void setTotalMemory(long totalMemory) {
        this.totalMemory = totalMemory;
    }

    public void setFreeMemory(long freeMemory) {
        this.freeMemory = freeMemory;
    }

    public void setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public void setAvailableProcessors(int availableProcessors) {
        this.availableProcessors = availableProcessors;
    }

    public void addSystemProps(Map systemProps) {
        this.systemProps.putAll(systemProps);
    }

    public Set<Integer> getPartitions() {
        return partitions;
    }

    public void setPartitions(Set<Integer> partitions) {
        this.partitions = partitions;
    }
}
