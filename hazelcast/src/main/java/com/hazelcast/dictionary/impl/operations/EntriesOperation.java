package com.hazelcast.dictionary.impl.operations;

import com.hazelcast.dictionary.impl.Segment;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.CallStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.hazelcast.dictionary.impl.DictionaryDataSerializerHook.ENTRIES_OPERATION;
import static com.hazelcast.spi.CallStatus.DONE_RESPONSE;

public class EntriesOperation extends DictionaryOperation {

    private int segmentId;
    private transient List<Map.Entry> response;

    public EntriesOperation() {
    }

    public EntriesOperation(String name, int segmentId) {
        super(name);
        this.segmentId = segmentId;
    }

    @Override
    public CallStatus call() throws Exception {
        Segment segment = partition.segments()[segmentId];
        response = segment.entries();
        return DONE_RESPONSE;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return ENTRIES_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(segmentId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        segmentId = in.readInt();
    }
}
