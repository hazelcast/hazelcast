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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.process.ProcessJob;
import com.hazelcast.spi.NodeEngine;

class NodeJobTracker extends AbstractJobTracker {

    private final NodeEngine nodeEngine;

    NodeJobTracker(String name, JobTrackerConfig jobTrackerConfig,
                   NodeEngine nodeEngine, HazelcastInstance hazelcastInstance) {
        super(name, jobTrackerConfig, hazelcastInstance);
        this.nodeEngine = nodeEngine;
    }

    @Override
    public <K, V> Job<K, V> newJob(KeyValueSource<K, V> source) {
        return new KeyValueJob<K, V>(name, nodeEngine, source, hazelcastInstance);
    }

    @Override
    public <K, V> ProcessJob<K, V> newProcessJob(KeyValueSource<K, V> source) {
        // TODO Implementation of process missing
        throw new UnsupportedOperationException("mapreduce process system not yet implemented");
    }

}
