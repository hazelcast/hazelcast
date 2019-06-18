package com.hazelcast.internal.query.operation;

import com.hazelcast.internal.query.QueryCancelReason;
import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.QueryService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class QueryCancelRequestOperation extends QueryAbstractOperation {

    private QueryId queryId;
    private QueryCancelReason reason;
    private String errMsg;

    @Override
    public void run() throws Exception {
        QueryService service = getService();

        service.onQueryCancelRequest(queryId, reason, errMsg);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        // TODO: Optimized serialziation
        out.writeObject(queryId);
        out.writeObject(reason);
        out.writeUTF(errMsg);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        // TODO: Optimized serialziation
        queryId = in.readObject();
        reason = in.readObject();
        errMsg = in.readUTF();
    }
}
