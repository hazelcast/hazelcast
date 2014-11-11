package com.hazelcast.partition.impl;

import com.hazelcast.partition.InternalPartition;

import java.util.Arrays;

import static java.lang.System.arraycopy;

final class PartitionReplicaVersions {
    final int partitionId;
    // read and updated only by operation/partition threads
    final long[] versions = new long[InternalPartition.MAX_BACKUP_COUNT];

    PartitionReplicaVersions(int partitionId) {
        this.partitionId = partitionId;
    }

    long[] incrementAndGet(int backupCount) {
        for (int i = 0; i < backupCount; i++) {
            versions[i]++;
        }
        return versions;
    }

    long[] get() {
        return versions;
    }

    boolean update(long[] newVersions, int currentReplica) {
        int index = currentReplica - 1;
        long current = versions[index];
        long next = newVersions[index];
        boolean valid = (current == next - 1);
        if (valid) {
            set(newVersions, currentReplica);
            current = next;
        }
        return current >= next;
    }

    void set(long[] newVersions, int fromReplica) {
        int fromIndex = fromReplica - 1;
        int len = newVersions.length - fromIndex;
        arraycopy(newVersions, fromIndex, versions, fromIndex, len);
    }

    void clear() {
        for (int i = 0; i < versions.length; i++) {
            versions[i] = 0;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PartitionReplicaVersions");
        sb.append("{partitionId=").append(partitionId);
        sb.append(", versions=").append(Arrays.toString(versions));
        sb.append('}');
        return sb.toString();
    }
}
