/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

public class Partitions implements ChangeEvent, Serializable {
    private int clusterId;
    private Date date;
    private Map<String, String> partitions = new HashMap<String, String>();

    private Map<String, Integer> count = new HashMap<String, Integer>();

    public Partitions() {
    }

    public Partitions(int clusterId) {
        this.clusterId = clusterId;
        date = new Date();
    }

    public ChangeEventType getChangeEventType() {
        return ChangeEventType.PARTITIONS;
    }

    public int getClusterId() {
        return clusterId;
    }

    public Date getCreatedDate() {
        return date;
    }

    public Map<String, String> getPartitions() {
        return partitions;
    }

    public Map<String, Integer> getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "Partitions{" +
                "clusterId=" + clusterId +
                ", date=" + date +
                ", count=" + count +
                '}';
    }
}

