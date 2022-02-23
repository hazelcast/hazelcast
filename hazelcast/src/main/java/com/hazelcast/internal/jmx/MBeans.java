/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.jmx;

import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicProxy;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;

import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.MapUtil.createConcurrentHashMap;

/**
 * A helper class which contains various types of {@link HazelcastMBean} factory methods and metadata.
 */
final class MBeans {

    private static final ConcurrentMap<String, MBeanFactory> MBEAN_FACTORY_TYPES_REGISTRY
            = createConcurrentHashMap(MBeanFactory.values().length);

    static {
        MBeanFactory[] mBeanFactories = MBeanFactory.values();
        for (MBeanFactory mBeanFactory : mBeanFactories) {
            MBEAN_FACTORY_TYPES_REGISTRY.put(mBeanFactory.getServiceName(), mBeanFactory);
        }
    }

    private MBeans() {
    }

    static HazelcastMBean createHazelcastMBeanOrNull(DistributedObject distributedObject,
                                                     ManagementService managementService) {
        MBeanFactory mBeanFactory = getMBeanFactory(distributedObject.getServiceName());
        return mBeanFactory == null ? null : mBeanFactory.createNew(distributedObject, managementService);
    }

    static String getObjectTypeOrNull(String serviceName) {
        MBeanFactory mBeanFactory = getMBeanFactory(serviceName);
        return mBeanFactory == null ? null : mBeanFactory.getObjectType();
    }

    private static MBeanFactory getMBeanFactory(String serviceName) {
        return MBEAN_FACTORY_TYPES_REGISTRY.get(serviceName);
    }

    enum MBeanFactory {

        MAP {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new MapMBean((IMap) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "IMap";
            }

            @Override
            public String getServiceName() {
                return MapService.SERVICE_NAME;
            }
        },

        LIST {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new ListMBean((IList) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "IList";
            }

            @Override
            public String getServiceName() {
                return ListService.SERVICE_NAME;
            }
        },

        MULTI_MAP {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new MultiMapMBean((MultiMap) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "MultiMap";
            }

            @Override
            public String getServiceName() {
                return MultiMapService.SERVICE_NAME;
            }
        },

        QUEUE {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new QueueMBean((IQueue) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "IQueue";
            }

            @Override
            public String getServiceName() {
                return QueueService.SERVICE_NAME;
            }
        },

        EXECUTOR_SERVICE {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new ExecutorServiceMBean((IExecutorService) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "IExecutorService";
            }

            @Override
            public String getServiceName() {
                return DistributedExecutorService.SERVICE_NAME;
            }
        },

        SET {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new SetMBean((ISet) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "ISet";
            }

            @Override
            public String getServiceName() {
                return SetService.SERVICE_NAME;
            }
        },

        TOPIC {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new TopicMBean((ITopic) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "ITopic";
            }

            @Override
            public String getServiceName() {
                return TopicService.SERVICE_NAME;
            }
        },

        REPLICATED_MAP {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new ReplicatedMapMBean((ReplicatedMapProxy) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "ReplicatedMap";
            }

            @Override
            public String getServiceName() {
                return ReplicatedMapService.SERVICE_NAME;
            }
        },

        RELIABLE_TOPIC {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new ReliableTopicMBean((ReliableTopicProxy) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "ReliableTopic";
            }

            @Override
            public String getServiceName() {
                return ReliableTopicService.SERVICE_NAME;
            }
        };

        abstract HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService);

        abstract String getObjectType();

        abstract String getServiceName();
    }
}
