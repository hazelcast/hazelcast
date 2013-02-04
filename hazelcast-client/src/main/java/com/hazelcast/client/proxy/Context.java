/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.proxy;

import com.hazelcast.client.Connection;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Context {
    private Connection connection;

    //Context will be accessed using threadlocal. Here I am using AtomicInteger as a mutable Integer. 
    // Not for being thread safe.
    private Map<String, AtomicInteger> counterMap = new HashMap<String, AtomicInteger>(0);

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public int incrementAndGet(String name, long hash) {
        String key = append(name, hash);
        AtomicInteger i = counterMap.get(key);
        if (i == null) {
            i = new AtomicInteger(0);
            counterMap.put(key, i);
        }
        return i.incrementAndGet();
    }

    private String append(String name, long hash) {
        return name == null ? "" : name + hash;
    }

    public int decrementAndGet(String name, long hash) {
        String key = append(name, hash);
        AtomicInteger i = counterMap.get(key);
        int currentCount = (i == null) ? -1 : i.decrementAndGet();
        if (currentCount == 0) {
            counterMap.remove(key);
        }
        return currentCount;
    }

    public boolean noMoreLocks() {
        return counterMap.size() == 0;
    }
}

