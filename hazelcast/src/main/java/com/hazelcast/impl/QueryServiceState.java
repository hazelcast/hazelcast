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

import java.util.HashMap;
import java.util.Map;

public class QueryServiceState implements Processable {

    Map<String, IndexRegionState> mapRegionStats = new HashMap<String, IndexRegionState>();
    final ConcurrentMapManager concurrentMapManager;
    private int queueSize = 0;

    public QueryServiceState(ConcurrentMapManager concurrentMapManager) {
        this.concurrentMapManager = concurrentMapManager;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public void addIndexRegionState(IndexRegionState indexRegionState) {
        mapRegionStats.put(indexRegionState.name, indexRegionState);
    }

    public void process() {
        concurrentMapManager.queryServiceState = this;
    }

    public void appendState(String name, StringBuffer sbState) {
        IndexRegionState region = mapRegionStats.get(name);
        if (region != null) {
            region.appendState(sbState);
        }
    }

    public static class IndexRegionState {
        public String name;
        public int ownedRecordCount;
        public int valueIndexCount;
        public IndexState[] indexStats;

        public IndexRegionState(String name, int ownedRecordCount, int valueIndexCount, IndexState[] indexStats) {
            this.name = name;
            this.ownedRecordCount = ownedRecordCount;
            this.valueIndexCount = valueIndexCount;
            this.indexStats = indexStats;
        }

        public void appendState(StringBuffer sbState) {
            sbState.append(", q.owned:");
            sbState.append(ownedRecordCount);
            sbState.append(", q.values:");
            sbState.append(valueIndexCount);
        }
    }

    public static class IndexState {
        public int indexCount;

        public IndexState(int indexCount) {
            this.indexCount = indexCount;
        }
    }
}
