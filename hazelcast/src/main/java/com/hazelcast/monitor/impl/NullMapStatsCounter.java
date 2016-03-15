/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor.impl;

import com.hazelcast.monitor.MapStatsCounter;

public class NullMapStatsCounter implements MapStatsCounter {

    public static final MapStatsCounter INSTANCE = new NullMapStatsCounter();

    @Override
    public void updateContainsStats(long startTime) {

    }

    @Override
    public void updateGetStats(long startTime, Object result) {

    }

    @Override
    public void updateGetStats(long startTime, long hits) {

    }

    @Override
    public void updatePutStats(long now) {

    }

    @Override
    public void updatePutStats(long latency, long putCount) {

    }

    @Override
    public void updateEntryProcessorStats(long startTime) {

    }

    @Override
    public void updateRemoveStats(long startTime) {

    }

    @Override
    public void updatePutIfAbsentStats(long startTime, Object result) {

    }
}
