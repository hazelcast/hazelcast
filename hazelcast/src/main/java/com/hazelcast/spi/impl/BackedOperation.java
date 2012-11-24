package com.hazelcast.spi.impl;

import com.hazelcast.spi.Operation;

/**
 * @mdogan 11/23/12
 */
public interface BackedOperation {

    public Operation getBackupOperation();

}
