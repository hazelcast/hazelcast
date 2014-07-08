package com.hazelcast.client.helpers;

import com.hazelcast.map.MapInterceptor;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;


/**
 * User: danny Date: 11/26/13
 */

public class SimpleClientInterceptor implements MapInterceptor, Portable {

    public static final int ID = 345;

    @Override
    public Object interceptGet(Object value) {
        if (value == null) {
            return null;
        }
        return value + ":";
    }

    @Override
    public void afterGet(Object value) {
    }

    @Override
    public Object interceptPut(Object oldValue, Object newValue) {
        return newValue.toString().toUpperCase();
    }

    @Override
    public void afterPut(Object value) {
    }

    @Override
    public Object interceptRemove(Object removedValue) {
        if (removedValue.equals("ISTANBUL")) {
            throw new RuntimeException("you can not remove this");
        }
        return removedValue;
    }

    @Override
    public void afterRemove(Object value) {
    }

    @Override
    public int getFactoryId() {
        return PortableHelpersFactory.ID;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getClassId() {
        return ID;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}

