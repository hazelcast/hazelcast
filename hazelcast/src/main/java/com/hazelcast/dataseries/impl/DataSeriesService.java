/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dataseries.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ConstructorFunction;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class DataSeriesService implements ManagedService, RemoteService {
    public static final String SERVICE_NAME = "hz:impl:dataSeriesService";
    private final ConcurrentMap<String, DataSeriesContainer> containers = new ConcurrentHashMap<String, DataSeriesContainer>();

    private final ConstructorFunction<String, DataSeriesContainer> containerConstructorFunction =
            new ConstructorFunction<String, DataSeriesContainer>() {
                public DataSeriesContainer createNew(String key) {
                    Config config = nodeEngine.getConfig();
                    DataSeriesConfig dataSeriesConfig = config.findDataSeriesConfig(key);
                    return new DataSeriesContainer(dataSeriesConfig, nodeEngine);
                }
            };

    private NodeEngineImpl nodeEngine;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    public DataSeriesContainer getDataSeriesContainer(String name) {
        return getOrPutIfAbsent(containers, name, containerConstructorFunction);
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        return new DataSeriesProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
    }
}
