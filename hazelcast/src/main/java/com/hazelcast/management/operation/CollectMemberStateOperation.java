package com.hazelcast.management.operation;

import com.hazelcast.instance.Node;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import java.io.IOException;

public class CollectMemberStateOperation extends Operation {

    TimedMemberState result;

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final Node node = nodeEngine.getNode();
        final ManagementCenterService managementCenterService = node.getManagementCenterService();
        if (managementCenterService == null) {
            // management center service might not be initialized
            result = null;
            return;
        }
        final TimedMemberState memberState = managementCenterService.createMemberState();
        result = memberState;
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

    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {

    }
}
