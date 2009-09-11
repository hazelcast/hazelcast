/**
 * 
 */
package com.hazelcast.client.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.client.nio.Address;
import com.hazelcast.client.nio.DataSerializable;

public class Bind implements DataSerializable{
	Address address;
	
    public Bind() {
    }

    public Bind(Address localAddress) {
      address = localAddress;
    }

    public String toString() {
        return "Bind " + address;
    }

    public void readData(final DataInput in) throws IOException {
        address = new Address();
        address.readData(in);
    }

    /* (non-Javadoc)
     * @see com.hazelcast.cluster.AbstractRemotelyProcessable#writeData(java.io.DataOutput)
     */
    public void writeData(final DataOutput out) throws IOException {
        address.writeData(out);
    }
    
}