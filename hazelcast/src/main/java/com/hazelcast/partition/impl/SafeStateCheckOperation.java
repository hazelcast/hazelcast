package com.hazelcast.partition.impl;

import com.hazelcast.spi.AbstractOperation;

/**
 * Checks whether a node is safe or not.
 * Safe means, first backups of partitions those owned by local member are sync with primary.
 *
 * @see com.hazelcast.core.PartitionService#isClusterSafe
 * @see com.hazelcast.core.PartitionService#isMemberSafe
 */
public class SafeStateCheckOperation extends AbstractOperation {

    private transient boolean safe;

    @Override
    public void run() throws Exception {
        final InternalPartitionServiceImpl service = getService();
        safe = service.getNode().getPartitionService().isMemberStateSafe();
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return safe;
    }

}
