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

package com.hazelcast.internal.usercodedeployment.impl;

import java.util.HashMap;
import java.util.Map;

/**
 * Thread-Local Class Cache is useful when the regular class-cache is disabled - we want to keep classes cached
 * at very least for the duration of a single deserialization request. Otherwise things may get funky with e.g.
 * class hierarchies.
 */
public final class ThreadLocalClassCache {

    public static final ThreadLocal<ThreadLocalClassCache> THREAD_LOCAL_CLASS_CACHE = new ThreadLocal<ThreadLocalClassCache>();

    private int counter = 1;
    private Map<String, ClassSource> map = new HashMap<String, ClassSource>();

    private ThreadLocalClassCache() {
    }

    private int decCounter() {
        counter--;
        return counter;
    }

    private void incCounter() {
        counter++;
    }

    public static void onStartDeserialization() {
        ThreadLocalClassCache threadLocalClassCache = THREAD_LOCAL_CLASS_CACHE.get();
        if (threadLocalClassCache != null) {
            threadLocalClassCache.incCounter();
        }
    }

    public static void onFinishDeserialization() {
        ThreadLocalClassCache threadLocalClassCache = THREAD_LOCAL_CLASS_CACHE.get();
        if (threadLocalClassCache != null && threadLocalClassCache.decCounter() == 0) {
            THREAD_LOCAL_CLASS_CACHE.remove();
        }
    }

    public static void store(String name, ClassSource classSource) {
        ThreadLocalClassCache threadLocalClassCache = THREAD_LOCAL_CLASS_CACHE.get();
        if (threadLocalClassCache == null) {
            threadLocalClassCache = new ThreadLocalClassCache();
            THREAD_LOCAL_CLASS_CACHE.set(threadLocalClassCache);
        }
        threadLocalClassCache.map.put(name, classSource);
    }

    public static ClassSource getFromCache(String name) {
        ThreadLocalClassCache threadLocalClassCache = THREAD_LOCAL_CLASS_CACHE.get();
        if (threadLocalClassCache != null) {
            return threadLocalClassCache.map.get(name);
        }
        return null;
    }
}
