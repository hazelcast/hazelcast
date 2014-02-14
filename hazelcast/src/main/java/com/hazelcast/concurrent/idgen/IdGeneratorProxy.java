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

package com.hazelcast.concurrent.idgen;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IdGeneratorProxy extends AbstractDistributedObject<IdGeneratorService> implements IdGenerator {

    public static final int BLOCK_SIZE = 10000;

    private final String name;
    private final IAtomicLong blockGenerator;
    private final AtomicInteger residue = new AtomicInteger(BLOCK_SIZE);
    private final AtomicLong local = new AtomicLong(-1);

    public IdGeneratorProxy(IAtomicLong blockGenerator, String name,
                            NodeEngine nodeEngine, IdGeneratorService service) {
        super(nodeEngine, service);
        this.name = name;
        this.blockGenerator = blockGenerator;
    }

    @Override
    public boolean init(long id) {
        if (id <= 0) {
            return false;
        }
        long step = (id / BLOCK_SIZE);

        synchronized (this) {
            boolean init = blockGenerator.compareAndSet(0, step + 1);
            if (init) {
                local.set(step);
                residue.set((int) (id % BLOCK_SIZE) + 1);
            }
            return init;
        }
    }

    @Override
    public long newId() {
        int value = residue.getAndIncrement();
        if (value >= BLOCK_SIZE) {
            synchronized (this) {
                value = residue.get();
                if (value >= BLOCK_SIZE) {
                    local.set(blockGenerator.getAndIncrement());
                    residue.set(0);
                }
                //todo: we need to get rid of this.
                return newId();
            }
        }
        return local.get() * BLOCK_SIZE + value;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return IdGeneratorService.SERVICE_NAME;
    }

    @Override
    protected void postDestroy() {
        blockGenerator.destroy();

        //todo: this behavior is racy; imagine what happens when destroy is called by different threads
        local.set(-1);
        residue.set(BLOCK_SIZE);
    }
}
