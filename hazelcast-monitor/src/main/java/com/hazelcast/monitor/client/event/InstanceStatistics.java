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

import java.util.Collection;
import java.util.Date;

public abstract class InstanceStatistics implements ChangeEvent {
    protected long size;
    protected long totalOPS;
    protected String name;
    private int clusterId;
    private Date date;

    public InstanceStatistics() {
    }

    public InstanceStatistics(int clusterId) {
        this.clusterId = clusterId;
        this.date = new Date();
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getTotalOPS() {
        return totalOPS;
    }

    public int getClusterId() {
        return clusterId;
    }

    public Date getCreatedDate() {
        return date;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public abstract Collection<? extends LocalInstanceStatistics> getListOfLocalStats();

    @Override
    public String toString() {
        return "{" +
                "size=" + size +
                ", totalOPS=" + totalOPS +
                ", name='" + name + '\'' +
                ", clusterId=" + clusterId +
                ", date=" + date +
                '}';
    }
}
