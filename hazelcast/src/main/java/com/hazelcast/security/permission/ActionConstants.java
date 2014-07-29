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
import com.hazelcast.queue.QueueService;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.topic.impl.TopicService;

import java.security.Permission;

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

    private ActionConstants() {
    }

    public static Permission getPermission(String name, String serviceName, String... actions) {
        if (QueueService.SERVICE_NAME.equals(serviceName)) {
            return new QueuePermission(name, actions);
        } else if (MapService.SERVICE_NAME.equals(serviceName)) {
            return new MapPermission(name, actions);
        } else if (MultiMapService.SERVICE_NAME.equals(serviceName)) {
            return new MultiMapPermission(name, actions);
        } else if (ListService.SERVICE_NAME.equals(serviceName)) {
            return new ListPermission(name, actions);
        } else if (SetService.SERVICE_NAME.equals(serviceName)) {
            return new SetPermission(name, actions);
        } else if (AtomicLongService.SERVICE_NAME.equals(serviceName)) {
            return new AtomicLongPermission(name, actions);
        } else if (CountDownLatchService.SERVICE_NAME.equals(serviceName)) {
            return new CountDownLatchPermission(name, actions);
        } else if (SemaphoreService.SERVICE_NAME.equals(serviceName)) {
            return new SemaphorePermission(name, actions);
        } else if (TopicService.SERVICE_NAME.equals(serviceName)) {
            return new TopicPermission(name, actions);
        } else if (LockService.SERVICE_NAME.equals(serviceName)) {
            return new LockPermission(name, actions);
        } else if (DistributedExecutorService.SERVICE_NAME.equals(serviceName)) {
            return new ExecutorServicePermission(name, actions);
        } else if (IdGeneratorService.SERVICE_NAME.equals(serviceName)) {
            return new AtomicLongPermission(IdGeneratorService.ATOMIC_LONG_NAME + name, actions);
        } else if (MapReduceService.SERVICE_NAME.equals(serviceName)) {
            return new MapReducePermission(name, actions);
        } else if (ReplicatedMapService.SERVICE_NAME.equals(serviceName)) {
            return new ReplicatedMapPermission(name, actions);
        } else if (AtomicReferenceService.SERVICE_NAME.equals(serviceName)) {
            return new AtomicReferencePermission(name, actions);
        }
        throw new IllegalArgumentException("No service matched!!!");
    }

}
