/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.function;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier.Context;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.security.permission.ReliableTopicPermission;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.topic.ITopic;

import java.security.Permission;

import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_PUBLISH;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;

/**
 * Factory methods for functions which requires a permission to run.
 */
public final class SecuredFunctions {

    private SecuredFunctions() {
    }

    public static <K, V> FunctionEx<? super Context, IMap<K, V>> iMapFn(String name) {
        return new FunctionEx<Context, IMap<K, V>>() {
            @Override
            public IMap<K, V> applyEx(Context context) {
                return context.hazelcastInstance().getMap(name);
            }

            @Override
            public Permission permission() {
                return new MapPermission(name, ACTION_CREATE, ACTION_READ);
            }
        };
    }

    public static <K, V> FunctionEx<? super Context, ReplicatedMap<K, V>> replicatedMapFn(String name) {
        return new FunctionEx<Context, ReplicatedMap<K, V>>() {
            @Override
            public ReplicatedMap<K, V> applyEx(Context context) {
                return context.hazelcastInstance().getReplicatedMap(name);
            }

            @Override
            public Permission permission() {
                return new ReplicatedMapPermission(name, ACTION_CREATE, ACTION_READ);
            }
        };
    }

    public static <E> FunctionEx<Processor.Context, ITopic<E>> reliableTopicFn(String name) {
        return new FunctionEx<Processor.Context, ITopic<E>>() {
            @Override
            public ITopic<E> applyEx(Processor.Context context) {
                return context.hazelcastInstance().getReliableTopic(name);
            }

            @Override
            public Permission permission() {
                return new ReliableTopicPermission(name, ACTION_CREATE, ACTION_PUBLISH);
            }
        };
    }

    public static <S> BiFunctionEx<? super Processor.Context, Void, ? extends S> createServiceFn(
            FunctionEx<? super Processor.Context, ? extends S> createContextFn
    ) {
        return new BiFunctionEx<Processor.Context, Void, S>() {
            @Override
            public S applyEx(Processor.Context context, Void o) throws Exception {
                return createContextFn.applyEx(context);
            }

            @Override
            public Permission permission() {
                return createContextFn.permission();
            }
        };
    }

}
