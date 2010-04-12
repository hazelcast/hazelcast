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

package com.hazelcast.monitor.server.event;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.monitor.client.event.InstanceStatistics;

import java.util.ArrayList;
import java.util.List;

public abstract class InstanceStatisticsGenerator implements ChangeEventGenerator{

    private List<? super InstanceStatistics> list = new ArrayList();
    protected String name;
    protected int clusterId;
    protected HazelcastClient client;

    public InstanceStatisticsGenerator(String instanceName, HazelcastClient client, int clusterId) {
        this.name = instanceName;
        this.client = client;
        this.clusterId = clusterId;
    }

    public List<? super InstanceStatistics> getPastStatistics() {
        return list;
    }

    void storeEvent(InstanceStatistics event) {
        List<? super InstanceStatistics> list = getPastStatistics();
        if (!list.isEmpty() && list.get(list.size() - 1).equals(event)) {
            list.remove(list.size() - 1);
        }
        list.add(event);
        while (list.size() > 100) {
            list.remove(0);
        }
    }

    public int getClusterId() {
        return clusterId;
    }

    public String getName() {
        return name;
    }
}
