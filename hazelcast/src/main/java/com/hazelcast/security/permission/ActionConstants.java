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

package com.hazelcast.security.permission;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.util.MapUtil;

import java.security.Permission;
import java.util.Collections;
import java.util.Map;

public final class ActionConstants {

    public static final String ACTION_ALL = "all";
    public static final String ACTION_CREATE = "create";
    public static final String ACTION_DESTROY = "destroy";
    public static final String ACTION_MODIFY = "modify";
    public static final String ACTION_READ = "read";
    public static final String ACTION_REMOVE = "remove";
    public static final String ACTION_LOCK = "lock";
    public static final String ACTION_LISTEN = "listen";
    public static final String ACTION_RELEASE = "release";
    public static final String ACTION_ACQUIRE = "acquire";
    public static final String ACTION_PUT = "put";
    public static final String ACTION_ADD = "add";
    public static final String ACTION_INDEX = "index";
    public static final String ACTION_INTERCEPT = "intercept";
    public static final String ACTION_PUBLISH = "publish";
    public static final String ACTION_AGGREGATE = "aggregate";
    public static final String ACTION_PROJECTION = "projection";

    public static final String LISTENER_INSTANCE = "instance";
    public static final String LISTENER_MEMBER = "member";
    public static final String LISTENER_MIGRATION = "migration";

    private static final Map<String, PermissionFactory> PERMISSION_FACTORY_MAP;

    static {
        final Map<String, PermissionFactory> permissionFactory = MapUtil.createHashMap(19);
        permissionFactory.put(QueueService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new QueuePermission(name, actions);
            }
        });
        permissionFactory.put(MapService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new MapPermission(name, actions);
            }
        });
        permissionFactory.put(MultiMapService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new MultiMapPermission(name, actions);
            }
        });
        permissionFactory.put(ListService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new ListPermission(name, actions);
            }
        });
        permissionFactory.put(SetService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new SetPermission(name, actions);
            }
        });
        permissionFactory.put(AtomicLongService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new AtomicLongPermission(name, actions);
            }
        });
        permissionFactory.put(CountDownLatchService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new CountDownLatchPermission(name, actions);
            }
        });
        permissionFactory.put(SemaphoreService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new SemaphorePermission(name, actions);
            }
        });
        permissionFactory.put(TopicService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new TopicPermission(name, actions);
            }
        });
        permissionFactory.put(LockService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new LockPermission(name, actions);
            }
        });
        permissionFactory.put(DistributedExecutorService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new ExecutorServicePermission(name, actions);
            }
        });
        permissionFactory.put(IdGeneratorService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new AtomicLongPermission(IdGeneratorService.ATOMIC_LONG_NAME + name, actions);
            }
        });
        permissionFactory.put(MapReduceService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new MapReducePermission(name, actions);
            }
        });
        permissionFactory.put(ReplicatedMapService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new ReplicatedMapPermission(name, actions);
            }
        });
        permissionFactory.put(AtomicReferenceService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new AtomicReferencePermission(name, actions);
            }
        });
        permissionFactory.put(CacheService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new CachePermission(name, actions);
            }
        });
        permissionFactory.put(RingbufferService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new RingBufferPermission(name, actions);
            }
        });
        permissionFactory.put(DistributedDurableExecutorService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new DurableExecutorServicePermission(name, actions);
            }
        });
        permissionFactory.put(CardinalityEstimatorService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new CardinalityEstimatorPermission(name, actions);
            }
        });
        //NOTE: remember to adjust size of backing map when adding new constants
        PERMISSION_FACTORY_MAP = Collections.unmodifiableMap(permissionFactory);
    }

    private ActionConstants() {
    }

    private interface PermissionFactory {
        Permission create(String name, String... actions);
    }

    /**
     * Creates a permission
     *
     * @param name
     * @param serviceName
     * @param actions
     * @return the created Permission
     * @throws java.lang.IllegalArgumentException if there is no service found with the given serviceName.
     */
    public static Permission getPermission(String name, String serviceName, String... actions) {
        PermissionFactory permissionFactory = PERMISSION_FACTORY_MAP.get(serviceName);
        if (permissionFactory == null) {
            throw new IllegalArgumentException("No permissions found for service: " + serviceName);
        }

        return permissionFactory.create(name, actions);
    }

}
