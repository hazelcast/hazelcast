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

package com.hazelcast.security.permission;

import com.hazelcast.collection.list.ListService;
import com.hazelcast.collection.set.SetService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.map.MapService;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.queue.impl.QueueService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.topic.impl.TopicService;

import java.security.Permission;
import java.util.HashMap;
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
        PERMISSION_FACTORY_MAP.put(IdGeneratorService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new AtomicLongPermission(IdGeneratorService.ATOMIC_LONG_NAME + name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(MapReduceService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new MapReducePermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(ReplicatedMapService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new ReplicatedMapPermission(name, actions);
            }
        });
        PERMISSION_FACTORY_MAP.put(AtomicReferenceService.SERVICE_NAME, new PermissionFactory() {
            @Override
            public Permission create(String name, String... actions) {
                return new AtomicReferencePermission(name, actions);
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
