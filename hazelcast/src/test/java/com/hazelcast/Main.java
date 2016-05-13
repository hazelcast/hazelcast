package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastTestSupport;
import sun.misc.UUDecoder;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by alarmnummer on 5/13/16.
 */
public class Main {

    public static void main(String[] args){
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        InternalSerializationService serializationService = HazelcastTestSupport.getSerializationService(hz);

        Foo foo = new Foo();
        foo.uuidString = hz.getCluster().getLocalMember().getUuid();
        foo.uuid = UUID.randomUUID();

        serializationService.toBytes(foo);
    }

    static class Foo implements DataSerializable {
        private String uuidString;
        private UUID uuid;

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            ByteArrayObjectDataOutput bos = (ByteArrayObjectDataOutput)out;
            int before = bos.position();
            out.writeUTF(uuidString);
            int after = bos.position();

            System.out.println("size:"+(after-before));
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {

        }
    }
}
