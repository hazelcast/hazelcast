package com.hazelcast.spi;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class InvokeOnPartitionTest extends HazelcastTestSupport {

    @Test
    public void invokeLocal() throws Exception {
        invoke(true);
    }

    @Test
    public void invokeRemote() throws Exception {
        invoke(false);
    }

    public void invoke(boolean localCall) throws Exception {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance remote = instances[1];

        Node node = getNode(local);
        OperationService operationService = node.nodeEngine.getOperationService();

        SomeOperation op = new SomeOperation();

        String key;
        if (localCall) {
            key = generateKeyOwnedBy(local);
        } else {
            key = generateKeyOwnedBy(remote);
        }

        Future f = operationService.invokeOnPartition("foo", op, node.getPartitionService().getPartitionId(key));
        Object found = f.get();

        if(localCall) {
            assertEquals(getNode(local).getThisAddress(), found);
        }else{
            assertEquals(getNode(remote).getThisAddress(), found);
        }
    }

    public static class SomeOperation extends AbstractOperation implements PartitionAwareOperation {
        private String response;

        public SomeOperation() {
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public Object getResponse() {
            return getNodeEngine().getThisAddress();
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeUTF(response);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            response = in.readUTF();
        }
    }
}
