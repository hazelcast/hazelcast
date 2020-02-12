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

package com.hazelcast.internal.affinity;

import com.hazelcast.logging.ILogger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class AffinitySupportExtention {

    private final ILogger logger;
    private ConcurrentMap<ThreadAffinity, AffinityController> registry = new ConcurrentHashMap<>();

    public AffinitySupportExtention(ILogger logger) {
        this.logger = logger;
    }

    public AffinityController create(Class<? extends Thread> cls) {
        checkNotNull(cls);
        ThreadAffinity type = cls.getAnnotation(ThreadAffinity.class);
        if (type != null) {
            return getOrPutIfAbsent(registry, type, key -> new AffinityController(logger, type));
        }

        return null;
    }

}
