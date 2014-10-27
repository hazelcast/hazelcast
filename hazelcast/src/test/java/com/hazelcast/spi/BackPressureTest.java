package com.hazelcast.spi;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class BackPressureTest extends HazelcastTestSupport {

    @Test
    @Ignore
    public void test() throws ExecutionException, InterruptedException {
        HazelcastInstance[] cluster = this.createHazelcastInstanceFactory(5).newInstances();
        warmUpPartitions(cluster);
        HazelcastInstance hz1 = cluster[0];
        Node node = getNode(hz1);
        OperationService operationService = node.getNodeEngine().getOperationService();

        for(int k=0;k<100000000;k++){
            SomeOperation operation = new SomeOperation(0, 1, 1000000);
            Future f = operationService.invokeOnPartition("don'tcare", operation, 0);
            f.get();

            if(k%10000 == 0){
                System.out.println("At: "+k);
            }
        }
    }

    public static class SomeOperation extends AbstractOperation implements BackupAwareOperation, PartitionAwareOperation {
        private int syncBackupCount;
        private int asyncBackupCount;
        private long backupOperationDelayNanos;

        public SomeOperation(){}

        public SomeOperation(int syncBackupCount, int asyncBackupCount, long backupOperationDelayNanos) {
            this.syncBackupCount = syncBackupCount;
            this.asyncBackupCount = asyncBackupCount;
            this.backupOperationDelayNanos = backupOperationDelayNanos;
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return syncBackupCount;
        }

        @Override
        public int getAsyncBackupCount() {
            return asyncBackupCount;
        }

        @Override
        public Operation getBackupOperation() {
            SomeBackupOperation someBackupOperation = new SomeBackupOperation(backupOperationDelayNanos);
            someBackupOperation.setPartitionId(getPartitionId());
            return someBackupOperation;
        }

        @Override
        public void run() throws Exception {
            //do nothing
         }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeInt(syncBackupCount);
            out.writeInt(asyncBackupCount);
            out.writeLong(backupOperationDelayNanos);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);

            syncBackupCount = in.readInt();
            asyncBackupCount = in.readInt();
            backupOperationDelayNanos = in.readLong();
        }
    }

    public static class SomeBackupOperation extends AbstractOperation implements BackupOperation, PartitionAwareOperation {
        private long delayNs;

        public SomeBackupOperation(){}

        public SomeBackupOperation(long delayNs) {
            this.delayNs = delayNs;
        }

        @Override
        public void run() throws Exception {
            LockSupport.parkNanos(delayNs);
         }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeLong(delayNs);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            delayNs = in.readLong();
        }
    }
}
