package com.hazelcast.client.helpers;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

/**
 * Created with IntelliJ IDEA.
 * User: danny
 * Date: 11/25/13
 * Time: 9:41 PM
 * To change this template use File | Settings | File Templates.
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
