/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc.server;

import java.util.function.Supplier;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * An Allocator for {@link Cmd} instances. Should be confined within a single
 * {@link RequestProcessor}.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class CmdAllocator {

    private final Pool[] pools;
    private final CmdRegistry cmdRegistry;
    private final int poolCapacity;

    public CmdAllocator(CmdRegistry cmdRegistry, int poolCapacity) {
        this.cmdRegistry = checkNotNull(cmdRegistry, "cmdRegistry");
        this.poolCapacity = checkNotNegative(poolCapacity, "poolCapacity");
        int capacity = cmdRegistry.capacity();
        this.pools = new Pool[capacity];
        for (int k = 0; k < capacity; k++) {
            CmdRegistry.Entry entry = cmdRegistry.get(k);
            if (entry != null) {
                pools[k] = new Pool(entry.getSupplier(), poolCapacity);
            }
        }
    }

    public final Cmd allocate(int cmdId) {
        Pool pool = pools[cmdId];
        if (pool == null) {
            CmdRegistry.Entry entry = cmdRegistry.get(cmdId);
            if (entry == null) {
                throw new IllegalArgumentException("Could not find registered command, for cmdId " + cmdId);
            }
            pools[cmdId] = new Pool(entry.getSupplier(), poolCapacity);
        }

        pool.allocated++;
        Cmd cmd;
        if (pool.index == -1) {
            cmd = pool.supplier.get();
            cmd.allocator = this;
            init(cmd);
        } else {
            cmd = pool.array[pool.index];
            //not needed
            pool.array[pool.index] = null;
            pool.index--;
            pool.allocatedFromPool++;
        }
//
//        if (pool.allocated % 1000000 == 0) {
//            System.out.println("allocate pooled percentage: " +
//                    ((pool.allocatedFromPool * 100f) / pool.allocated) + " %, dropped:"+ pool.dropped);
//        }

        return cmd;
    }

    public void init(Cmd op) {
        op.init();
    }

    public final void free(Cmd cmd) {
        Pool pool = pools[cmd.id];
        if (pool.index == pool.array.length - 1) {
            pool.dropped++;
            return;
        }

        cmd.clear();
        cmd.request = null;
        cmd.response = null;
        pool.index++;
        pool.array[pool.index] = cmd;
    }

    private static final class Pool {
        public long dropped;
        // index points to first item that can be removed.
        private int index = -1;
        private final Cmd[] array;
        private final Supplier<? extends Cmd> supplier;
        private long allocatedFromPool;
        private long allocated;

        private Pool(Supplier<? extends Cmd> supplier, int poolCapacity) {
            this.array = new Cmd[poolCapacity];
            this.supplier = supplier;
        }
    }
}
