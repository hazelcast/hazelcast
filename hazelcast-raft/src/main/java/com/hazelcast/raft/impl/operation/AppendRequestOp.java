package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.service.RaftService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendRequestOp extends AsyncRaftOp {

    private AppendRequest appendRequest;

    public AppendRequestOp() {
    }

    public AppendRequestOp(String name, AppendRequest appendRequest) {
        super(name);
        this.appendRequest = appendRequest;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handleAppendEntries(name, appendRequest);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(appendRequest);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        appendRequest = in.readObject();
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPEND_REQUEST_OP;
    }
}
