package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.requestservice.Op;
import com.hazelcast.spi.impl.requestservice.OpCodes;
import io.netty.incubator.channel.uring.IOUringReactor;

import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_REQUEST_CALL_ID;

public class RandomLoadOp extends Op {

    private boolean loaded;
    public RandomLoadOp() {
        super(OpCodes.TABLE_NOOP);
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
        if(!loaded){
            IOUringReactor reactor = (IOUringReactor) this.scheduler.getReactor();
            short someId = 0;
            int fd = 0;
            reactor.sq_addRead(fd, 0, 0,0, someId);
            return Op.BLOCKED;
        }else {
            response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQUEST_CALL_ID))
                    .completeWriting();

            return Op.COMPLETED;
        }
    }
}
