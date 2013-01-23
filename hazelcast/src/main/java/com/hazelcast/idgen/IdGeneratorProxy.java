/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.idgen;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ali 1/23/13
 */
public class IdGeneratorProxy implements IdGenerator {

    public static final String ATOMIC_NUMBER_NAME = "hz:atomic:idGenerator:";

    private static final int BLOCK_SIZE = 1000;

    final NodeEngine nodeEngine;

    final String name;

    final AtomicNumber atomicNumber;

    AtomicInteger residue;

    AtomicLong local;

    private final Object syncObject = new Object();

    public IdGeneratorProxy(NodeEngine nodeEngine, String name, AtomicNumber atomicNumber) {
        this.nodeEngine = nodeEngine;
        this.name = name;
        this.atomicNumber = atomicNumber;
        residue = new AtomicInteger(BLOCK_SIZE);
        local = new AtomicLong(-1);
    }

    public boolean init(long id) {
        if (id <= 0) {
            return false;
        }
        long step = (id / BLOCK_SIZE) + 1;

        synchronized (syncObject) {
            return atomicNumber.compareAndSet(0, step);
        }

    }

    public long newId() {
        int value = residue.getAndIncrement();
        if (value >= BLOCK_SIZE) {
            synchronized (syncObject) {
                value = residue.get();
                if (value >= BLOCK_SIZE) {
                    local.set(atomicNumber.getAndIncrement());
                    residue.set(0);
                }
                return newId();
            }
        }
        return local.get() * BLOCK_SIZE + value;
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        System.out.println("atomic: " + atomicNumber.get() + ", local: " + local.get());
        return name;
    }

    public void destroy() {

    }
}
