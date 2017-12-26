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

package com.hazelcast.dictionary.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.codeneneration.Compiler;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ConstructorFunction;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class DictionaryService implements ManagedService, RemoteService {
    public static final String SERVICE_NAME = "hz:impl:dictionaryService";

    private final Compiler compiler = new Compiler("dictionary-src");
    private final ConcurrentMap<String, DictionaryContainer> containers = new ConcurrentHashMap<>();

    private final ConstructorFunction<String, DictionaryContainer> containerConstructorFunction =
            new ConstructorFunction<String, DictionaryContainer>() {
                public DictionaryContainer createNew(String key) {
                    Config config = nodeEngine.getConfig();
                    DictionaryConfig dictionaryConfig = config.findDictionaryConfig(key);

                    checkConfig(key, dictionaryConfig);
                    return new DictionaryContainer(dictionaryConfig, nodeEngine, compiler);
                }

                private void checkConfig(String key, DictionaryConfig dictionaryConfig) {
                        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
                        if(partitionCount==1){
                            // for testing
                            return;
                        }

                        if (dictionaryConfig.getSegmentsPerPartition() == partitionCount) {
                            throw new IllegalStateException("The segments per partitions of dictionary " + key + " can't be the "
                                    + "same as partition count");
                        }
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

    public DictionaryContainer getDictionaryContainer(String name) {
        return getOrPutIfAbsent(containers, name, containerConstructorFunction);
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        return new DictionaryProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
    }
}
