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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class IdGeneratorProxy
        extends AbstractDistributedObject<IdGeneratorService>
        implements IdGenerator {

    public static final int BLOCK_SIZE = 10000;

    private static final AtomicIntegerFieldUpdater<IdGeneratorProxy> RESIDUE_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(IdGeneratorProxy.class, "residue");
    private static final AtomicLongFieldUpdater<IdGeneratorProxy> LOCAL_UPDATER = AtomicLongFieldUpdater
            .newUpdater(IdGeneratorProxy.class, "local");

    private final String name;
    private final IAtomicLong blockGenerator;
    private volatile int residue = BLOCK_SIZE;
    private volatile long local = -1L;

    public IdGeneratorProxy(IAtomicLong blockGenerator, String name, NodeEngine nodeEngine, IdGeneratorService service) {
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
                LOCAL_UPDATER.set(this, step);
                RESIDUE_UPDATER.set(this, (int) (id % BLOCK_SIZE) + 1);
            }
            return init;
        }
    }

    @Override
    public long newId() {
        int value = RESIDUE_UPDATER.getAndIncrement(this);
        if (value >= BLOCK_SIZE) {
            synchronized (this) {
                value = residue;
                if (value >= BLOCK_SIZE) {
                    LOCAL_UPDATER.set(this, blockGenerator.getAndIncrement());
                    RESIDUE_UPDATER.set(this, 0);
                }
                //todo: we need to get rid of this.
                return newId();
            }
        }
        return local * BLOCK_SIZE + value;
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
        LOCAL_UPDATER.set(this, -1);
        RESIDUE_UPDATER.set(this, BLOCK_SIZE);
    }
}
