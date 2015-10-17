package com.hazelcast.nio.serialization;

import com.hazelcast.internal.serialization.ObjectDataInputStream;
import com.hazelcast.internal.serialization.ObjectDataOutputStream;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Test only helper util for serialization service
 */
public class SerializationServiceUtil {

    public static ObjectDataOutputStream createObjectDataOutputStream(OutputStream out, SerializationService ss) {
        return new ObjectDataOutputStream(out, ss);
    }

    public static ObjectDataInputStream createObjectDataInputStream(InputStream in, SerializationService ss) {
        return new ObjectDataInputStream(in, ss);
    }

}
