/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MultiMap;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.topic.impl.TopicService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A helper class which contains various types of {@link HazelcastMBean} factory methods and metadata.
 */
final class MBeans {

    private static final ConcurrentMap<String, MBeanFactory> MBEAN_FACTORY_TYPES_REGISTRY
            = new ConcurrentHashMap<String, MBeanFactory>(MBeanFactory.values().length);

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

        ATOMIC_LONG {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new AtomicLongMBean((IAtomicLong) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "IAtomicLong";
            }

            @Override
            public String getServiceName() {
                return AtomicLongService.SERVICE_NAME;
            }
        },

        ATOMIC_REFERENCE {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new AtomicReferenceMBean((IAtomicReference) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "IAtomicReference";
            }

            @Override
            public String getServiceName() {
                return AtomicReferenceService.SERVICE_NAME;
            }
        },

        COUNT_DOWN_LATCH {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new CountDownLatchMBean((ICountDownLatch) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "ICountDownLatch";
            }

            @Override
            public String getServiceName() {
                return CountDownLatchService.SERVICE_NAME;
            }
        },

        LOCK {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new LockMBean((ILock) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "ILock";
            }

            @Override
            public String getServiceName() {
                return LockService.SERVICE_NAME;
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

        SEMAPHORE {
            @Override
            public HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new SemaphoreMBean((ISemaphore) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "ISemaphore";
            }

            @Override
            public String getServiceName() {
                return SemaphoreService.SERVICE_NAME;
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
        };

        abstract HazelcastMBean createNew(DistributedObject distributedObject, ManagementService managementService);

        abstract String getObjectType();

        abstract String getServiceName();
    }
}
