/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IdGenerator;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IdGeneratorImpl implements IdGenerator {

    private static final long BILLION = 1 * 1000 * 1000;

    private final String name;

    public IdGeneratorImpl(String name) {
        this.name = name;
    }

    AtomicLong billion = new AtomicLong(-1);

    AtomicLong currentId = new AtomicLong(2 * BILLION);

    AtomicBoolean fetching = new AtomicBoolean(false);

    public long newId() {
        long billionNow = billion.get();
        long idAddition = currentId.incrementAndGet();
        if (idAddition >= BILLION) {
            synchronized (this) {
                try {
                    billionNow = billion.get();
                    idAddition = currentId.incrementAndGet();
                    if (idAddition >= BILLION) {
                        Long idBillion = getNewBillion();
                        long newBillion = idBillion.longValue() * BILLION;
                        billion.set(newBillion);
                        currentId.set(0);
                    }
                    billionNow = billion.get();
                    idAddition = currentId.incrementAndGet();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }

        }
        long result = billionNow + idAddition;
        return result;
    }

    private Long getNewBillion() {
        try {
            DistributedTask<Long> task = new DistributedTask<Long>(new IncrementTask(name));
            FactoryImpl.executorServiceImpl.execute(task);
            return task.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static class IncrementTask implements Callable<Long>, Serializable {
        private static final String ID_GENERATOR_MAP_NAME = "__hz_IdGenerator";
        String name = null;

        public IncrementTask() {
            super();
        }

        public IncrementTask(String uuidName) {
            super();
            this.name = uuidName;
        }

        public Long call() {
            IMap<String, Long> map = Hazelcast.getMap(ID_GENERATOR_MAP_NAME);
            map.lock(name);
            try {
                Long max = (Long) map.get(name);
                if (max == null) {
                    max = Long.valueOf(0l);
                    map.put(name, Long.valueOf(0));
                    return max;
                } else {
                    Long newMax = Long.valueOf(max.longValue() + 1);
                    map.put(name, newMax);
                    return newMax;
                }
            } finally {
                map.unlock(name);
            }
        }
    }
}
