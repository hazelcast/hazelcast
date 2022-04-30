package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.engine.iouring.IOUringScheduler;
import com.hazelcast.spi.impl.requestservice.Op;
import com.hazelcast.spi.impl.requestservice.OpCodes;
import com.hazelcast.spi.impl.engine.iouring.IOUringEventloop;

import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_REQ_CALL_ID;

public final class RandomLoadOp extends Op {

    private boolean loaded;

    public RandomLoadOp() {
        super(OpCodes.RANDOM_LOAD);
    }

    @Override
    public void clear() {
        super.clear();
        loaded = false;
    }

    public void handle_IORING_OP_READ(int res, int flags) {
        loaded = true;
        scheduler.schedule(this);
    }

    @Override
    public int run() throws Exception {
        if (!loaded) {
            IOUringEventloop eventloop = (IOUringEventloop) this.scheduler.getEventloop();
            eventloop.getDiskScheduler().scheduleLoad(IOUringScheduler.dummyFile, 0, 100, this);
            return BLOCKED;
        } else {
            response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQ_CALL_ID))
                    .writeComplete();

            return COMPLETED;
        }
    }
}
