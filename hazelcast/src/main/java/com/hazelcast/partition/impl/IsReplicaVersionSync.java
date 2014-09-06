package com.hazelcast.partition.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

/**
 * Queries if replica version is sync between partitions.
 */
public class IsReplicaVersionSync extends Operation implements PartitionAwareOperation, MigrationCycleOperation {

    private long version;
    private transient boolean result;


    public IsReplicaVersionSync() {
    }

    public IsReplicaVersionSync(long version) {
        this.version = version;
    }

    @Override
    public void beforeRun() throws Exception {

    }

    @Override
    public void run() throws Exception {
        final InternalPartitionServiceImpl partitionService = getService();
        final int partitionId = getPartitionId();
        final int replicaIndex = getReplicaIndex();
        final long[] currentVersions = partitionService.getPartitionReplicaVersions(partitionId);
        final long currentVersion = currentVersions[replicaIndex - 1];
        if (currentVersion == version) {
            result = true;
        }
    }

    @Override
    public void afterRun() throws Exception {

    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(version);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        version = in.readLong();
    }
}
