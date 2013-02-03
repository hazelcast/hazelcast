/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cluster;

import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The Class Master.
 */
public class Master extends AbstractRemotelyProcessable {

    /**
     * The address.
     */
    protected Address address = null;

    /**
     * Instantiates a new isMaster.
     */
    public Master() {
    }

    /**
     * Instantiates a new isMaster.
     *
     * @param originAddress the origin address
     */
    public Master(final Address originAddress) {
        super();
        this.address = originAddress;
    }
    /* (non-Javadoc)
    * @see com.hazelcast.cluster.AbstractRemotelyProcessable#readData(java.io.DataInput)
    */

    @Override
    public void readData(final DataInput in) throws IOException {
        address = new Address();
        address.readData(in);
    }
    /* (non-Javadoc)
    * @see com.hazelcast.cluster.AbstractRemotelyProcessable#writeData(java.io.DataOutput)
    */

    @Override
    public void writeData(final DataOutput out) throws IOException {
        address.writeData(out);
    }
    /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */

    @Override
    public String toString() {
        return "Master " + address;
    }
    /* (non-Javadoc)
    * @see com.hazelcast.impl.BaseManager.Processable#process()
    */

    public void process() {
        node.clusterManager.handleMaster(this);
    }

    /**
     * Gets the address.
     *
     * @return the address
     */
    public Address getAddress() {
        return address;
    }

    /**
     * Sets the address.
     *
     * @param address the address to set
     */
    public void setAddress(final Address address) {
        this.address = address;
    }
}
