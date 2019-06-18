package com.hazelcast.internal.query.operation;

import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.QueryService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class QueryCancelOperation extends QueryAbstractOperation {
    /** Query ID. */
    private QueryId queryId;

    public QueryId getQueryId() {
        return queryId;
    }

    @Override
    public void run() throws Exception {
        QueryService service = getService();

        //service.cancelQuery(queryId);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeObject(queryId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        queryId = in.readObject();
    }
}
