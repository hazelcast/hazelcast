package com.hazelcast.replicatedmap;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.record.Vector;

class DataReplicatedRecord extends ReplicatedRecord<Data> implements ReplicatedRecord<Data> {


    public DataReplicatedRecord() {
    }

    public DataReplicatedRecord(Data key, Data value, Vector vector, int hash) {
        super(value, vector, hash);
    }

}
