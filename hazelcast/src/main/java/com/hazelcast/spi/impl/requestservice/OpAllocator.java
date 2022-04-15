package com.hazelcast.spi.impl.requestservice;

import com.hazelcast.spi.impl.engine.frame.Frame;
import com.hazelcast.table.impl.NoOp;
import com.hazelcast.table.impl.SelectByKeyOperation;
import com.hazelcast.table.impl.UpsertOp;

import java.util.function.Supplier;

import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_REQUEST_OPCODE;

public final class OpAllocator {

    private final Pool[] pools;
    private final OpScheduler scheduler;
    private final Managers manager;

    public OpAllocator(OpScheduler scheduler, Managers managers) {
        this.scheduler = scheduler;
        this.manager = managers;

        pools = new Pool[OpCodes.MAX_OPCODE + 1];
        pools[0] = new Pool(UpsertOp::new);
        pools[1] = new Pool(SelectByKeyOperation::new);
        pools[2] = new Pool(NoOp::new);
    }

    public Op allocate(Frame request) {
        int opcode = request.getInt(OFFSET_REQUEST_OPCODE);
        Pool pool = pools[opcode];
        pool.allocated++;
        Op op;
        if (pool.index == -1) {
            op = pool.supplier.get();
            op.allocator = this;
            op.managers = manager;
            op.scheduler = scheduler;
        } else {
            op = pool.array[pool.index];
            pool.array[pool.index] = null;//not needed
            pool.index--;
            pool.allocatedFromPool++;
        }
//
//        if (pool.allocated % 1000000 == 0) {
//            System.out.println("allocate pooled percentage: " +
//                    ((pool.allocatedFromPool * 100f) / pool.allocated) + " %, dropped:"+ pool.dropped);
//        }

        return op;
    }

    public void free(Op op) {
        Pool pool = pools[op.opcode];
        if (pool.index == pool.array.length - 1) {
            pool.dropped++;
            return;
        }

        op.clear();
        op.request = null;
        op.response = null;
        pool.index++;
        pool.array[pool.index] = op;
    }

    private static class Pool {
        public long dropped;
        // index points to first item that can be removed.
        private int index = -1;
        private Op[] array = new Op[16384];
        private Supplier<Op> supplier;
        private long allocatedFromPool = 0;
        private long allocated = 0;

        private Pool(Supplier supplier) {
            this.supplier = supplier;
        }
    }
}
