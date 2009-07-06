/**
 * 
 */
package com.hazelcast.cluster;

import com.hazelcast.impl.FactoryImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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