/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService;
import com.hazelcast.internal.crdt.pncounter.PNCounterService;
import com.hazelcast.internal.locksupport.LockSupportService;
import com.hazelcast.internal.usercodedeployment.UserCodeDeploymentService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;

import java.security.Permission;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"checkstyle:executablestatementcount"})
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
    public static final String ACTION_USER_CODE_DEPLOY = "deploy";

    public static final String LISTENER_INSTANCE = "instance";
    public static final String LISTENER_MEMBER = "member";
    public static final String LISTENER_MIGRATION = "migration";

    private static final Map<String, PermissionFactory> PERMISSION_FACTORY_MAP = new HashMap<String, PermissionFactory>();

    static {
        PERMISSION_FACTORY_MAP.put(QueueService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new QueuePermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(MapService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new MapPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(MultiMapService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new MultiMapPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(ListService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new ListPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(SetService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new SetPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(AtomicLongService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new AtomicLongPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(CountDownLatchService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new CountDownLatchPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(SemaphoreService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new SemaphorePermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(TopicService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new TopicPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(LockSupportService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new LockPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(LockService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new LockPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(DistributedExecutorService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new ExecutorServicePermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(FlakeIdGeneratorService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new FlakeIdGeneratorPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(ReplicatedMapService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new ReplicatedMapPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(AtomicRefService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new AtomicReferencePermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(CacheService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new CachePermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(RingbufferService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new RingBufferPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(DistributedDurableExecutorService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new DurableExecutorServicePermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(CardinalityEstimatorService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new CardinalityEstimatorPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(UserCodeDeploymentService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new UserCodeDeploymentPermission(actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(PNCounterService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new PNCounterPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(ReliableTopicService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new ReliableTopicPermission(name, actions);
            }
        });
    }

    private ActionConstants() {
    }

    private interface PermissionFactory {
        Permission create(String name, String... actions);
    }

    /**
     * Creates a permission
     *
     * @param name        the permission name
     * @param serviceName the service name
     * @param actions     the actions
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
