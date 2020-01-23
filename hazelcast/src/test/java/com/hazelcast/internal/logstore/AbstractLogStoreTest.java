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

package com.hazelcast.internal.logstore;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import sun.misc.Unsafe;

import java.util.LinkedList;
import java.util.List;

public abstract class AbstractLogStoreTest extends HazelcastTestSupport {
    public final static Unsafe UNSAFE = UnsafeUtil.UNSAFE;

    private List<LogStore> stores = new LinkedList<>();

    @After
    public void tearDown() {
        for (LogStore store : stores) {
            store.clear();
        }
        stores.clear();
    }

    protected IntLogStore createIntLogStore(LogStoreConfig config) {
        IntLogStore store = new IntLogStore(config);
        stores.add(store);
        return store;
    }

    protected ByteLogStore createByteLogStore(LogStoreConfig config) {
        ByteLogStore store = new ByteLogStore(config);
        stores.add(store);
        return store;
    }

    protected LongLogStore createLongLogStore(LogStoreConfig config) {
        LongLogStore store = new LongLogStore(config);
        stores.add(store);
        return store;
    }

    protected <E> ObjectLogStore<E> createObjectLogStore(LogStoreConfig config) {
        ObjectLogStore<E> store = new ObjectLogStore<E>(config);
        stores.add(store);
        return store;
    }
}
