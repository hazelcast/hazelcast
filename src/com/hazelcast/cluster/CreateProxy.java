/**
 * 
 */
package com.hazelcast.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.impl.FactoryImpl;

public class CreateProxy extends AbstractRemotelyProcessable {

    public String name;

    public CreateProxy() {
    }

    public CreateProxy(String name) {
        this.name = name;
    }

    public void process() {
        FactoryImpl.createProxy(name);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public String toString() {
        return "CreateProxy [" + name + "]";
    }
}