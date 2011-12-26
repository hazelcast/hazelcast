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

package com.hazelcast.impl.management;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.CMap;
import com.hazelcast.impl.ConcurrentMapManager;
import com.hazelcast.impl.FactoryImpl;

public abstract class ClusterServiceCallable implements HazelcastInstanceAware {

    protected transient HazelcastInstance hazelcastInstance;

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    ConcurrentMapManager getConcurrentMapManager() {
        FactoryImpl factory = (FactoryImpl) hazelcastInstance;
        return factory.node.concurrentMapManager;
    }

    ClusterService getClusterService() {
        FactoryImpl factory = (FactoryImpl) hazelcastInstance;
        return factory.node.clusterService;
    }

    CMap getCMap(String mapName) {
        return getConcurrentMapManager().getOrCreateMap(Prefix.MAP + mapName);
    }
}
