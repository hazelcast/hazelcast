package com.hazelcast.console;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Echoes to screen
 */
public class Echo implements Callable<String>, DataSerializable, HazelcastInstanceAware {

    String input;
    private transient HazelcastInstance hz;

    public Echo(String input) {
        this.input = input;
    }

    @Override
    public String call() {
        hz.getCountDownLatch("latch").countDown();
        return hz.getCluster().getLocalMember().toString() + ":" + input;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(input);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        input = in.readUTF();
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hz = hazelcastInstance;
    }
}
