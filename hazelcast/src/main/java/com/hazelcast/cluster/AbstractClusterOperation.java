package com.hazelcast.cluster;

import com.hazelcast.spi.AbstractOperation;

/**
 * @mdogan 12/6/12
 */
abstract class AbstractClusterOperation extends AbstractOperation implements JoinOperation {

    @Override
    public boolean returnsResponse() {
        return false;
    }

}
