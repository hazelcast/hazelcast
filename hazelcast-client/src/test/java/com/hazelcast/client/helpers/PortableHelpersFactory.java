package com.hazelcast.client.helpers;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;


/**
 * User: danny Date: 11/26/13
 */

public class PortableHelpersFactory implements PortableFactory {

    public static final int ID = 666;

    public Portable create(int classId) {
        if (classId == SimpleClientInterceptor.ID){
            return new SimpleClientInterceptor();
        }

        return null;
    }
}
