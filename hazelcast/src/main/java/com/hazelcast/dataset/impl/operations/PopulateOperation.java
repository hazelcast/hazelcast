package com.hazelcast.dataset.impl.operations;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.Partition;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Iterator;

public class PopulateOperation extends DataSetOperation {

    private String srcName;

    public PopulateOperation() {
    }

    public PopulateOperation(String dstName, String srcName) {
        super(dstName);
        this.srcName = srcName;
    }

    @Override
    public void run() throws Exception {
        MapService mapService = getNodeEngine().getService(MapService.SERVICE_NAME);
        PartitionContainer partitionContainer = mapService.getMapServiceContext().getPartitionContainer(getPartitionId());
        RecordStore recordStore = partitionContainer.getRecordStore(srcName);
        Iterator<Record> it = recordStore.iterator();

        for (; it.hasNext(); ) {
            Record record = it.next();
            partition.insert(null, record.getValue());
        }
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.POPULATE_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeUTF(srcName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        srcName = in.readUTF();
    }
}
