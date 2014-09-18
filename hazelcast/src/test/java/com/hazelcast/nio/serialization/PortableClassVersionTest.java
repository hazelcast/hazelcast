package com.hazelcast.nio.serialization;

import com.hazelcast.client.impl.client.AuthenticationRequest;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Packet;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.serialization.PortableTest.createNamedPortableClassDefinition;
import static com.hazelcast.nio.serialization.PortableTest.createSerializationService;
import static com.hazelcast.nio.serialization.PortableTest.transferClassDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * User: sancar
 * Date: 21/05/14
 * Time: 17:17
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PortableClassVersionTest {

    static final int FACTORY_ID = PortableTest.FACTORY_ID;

    @Test
    public void testDifferentClassVersions() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        SerializationService serializationService2 = new DefaultSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        testDifferentClassVersions(serializationService, serializationService2);
    }

    @Test
    public void testDifferentClassAndServiceVersions() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().setVersion(1)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        SerializationService serializationService2 = new DefaultSerializationServiceBuilder().setVersion(2)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        testDifferentClassVersions(serializationService, serializationService2);
    }

    private void testDifferentClassVersions(SerializationService serializationService,
            SerializationService serializationService2) {

        NamedPortable p1 = new NamedPortable("named-portable", 123);
        Data data = serializationService.toData(p1);

        NamedPortableV2 p2 = new NamedPortableV2("named-portable", 123);
        Data data2 = serializationService2.toData(p2);

        transferClassDefinition(data, serializationService, serializationService2);
        NamedPortableV2 o1 = serializationService2.toObject(data);

        transferClassDefinition(data2, serializationService2, serializationService);
        NamedPortable o2 = serializationService.toObject(data2);

        assertEquals(o1.name, o2.name);
    }

    @Test
    public void testDifferentClassVersionsUsingDataWriteAndRead() throws IOException {
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        SerializationService serializationService2 = new DefaultSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        testDifferentClassVersionsUsingDataWriteAndRead(serializationService, serializationService2);
    }

    @Test
    public void testDifferentClassAndServiceVersionsUsingDataWriteAndRead() throws IOException {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().setVersion(1)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        SerializationService serializationService2 = new DefaultSerializationServiceBuilder().setVersion(2)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        testDifferentClassVersionsUsingDataWriteAndRead(serializationService, serializationService2);
    }

    private void testDifferentClassVersionsUsingDataWriteAndRead(SerializationService serializationService,
            SerializationService serializationService2) throws IOException {

        NamedPortable p1 = new NamedPortable("portable-v1", 111);
        Data data = serializationService.toData(p1);

        // register class def
        transferClassDefinition(data, serializationService, serializationService2);

        // emulate socket write by writing data to stream
        BufferObjectDataOutput out = serializationService.createObjectDataOutput(1024);
        out.writeData(data);
        byte[] bytes = out.toByteArray();
        byte[] header = ((PortableDataOutput) out).getPortableHeader();

        // emulate socket read by reading data from stream
        BufferObjectDataInput in = serializationService2.createObjectDataInput(new HeapData(0, bytes, 0, header));
        data = in.readData();

        // read data
        Object object1 = serializationService2.toObject(data);

        // serialize new portable version
        NamedPortableV2 p2 = new NamedPortableV2("portable-v2", 123);
        Data data2 = serializationService2.toData(p2);

        transferClassDefinition(data2, serializationService2, serializationService);
        // de-serialize back using old version
        Object object2 = serializationService.toObject(data2);

        assertTrue(object1 instanceof NamedPortableV2);
        assertTrue(object2 instanceof NamedPortable);
    }

    @Test
    public void testPreDefinedDifferentVersionsWithInnerPortable() {
        final SerializationService serializationService = createSerializationService(1);
        serializationService.getPortableContext().registerClassDefinition(createInnerPortableClassDefinition());

        final SerializationService serializationService2 = createSerializationService(2);
        serializationService2.getPortableContext().registerClassDefinition(createInnerPortableClassDefinition());

        NamedPortable[] nn = new NamedPortable[1];
        nn[0] = new NamedPortable("name", 123);
        InnerPortable inner = new InnerPortable(new byte[]{0, 1, 2}, new char[]{'c', 'h', 'a', 'r'},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, nn);

        final MainPortable mainWithInner = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", inner);

        testPreDefinedDifferentVersions(serializationService, serializationService2, mainWithInner);
    }

    @Test
    public void testPreDefinedDifferentVersionsWithNullInnerPortable() {
        ClassDefinition innerPortableClassDefinition = createInnerPortableClassDefinition();

        final SerializationService serializationService = createSerializationService(1);
        serializationService.getPortableContext().registerClassDefinition(innerPortableClassDefinition);

        final SerializationService serializationService2 = createSerializationService(2);
        serializationService2.getPortableContext().registerClassDefinition(innerPortableClassDefinition);

        final MainPortable mainWithNullInner = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", null);

        testPreDefinedDifferentVersions(serializationService, serializationService2, mainWithNullInner);
    }

    private void testPreDefinedDifferentVersions(SerializationService serializationService,
            SerializationService serializationService2, MainPortable mainPortable) {
        final Data data = serializationService.toData(mainPortable);

        transferClassDefinition(data, serializationService, serializationService2);
        assertEquals(mainPortable, serializationService2.toObject(data));
    }

    static ClassDefinition createInnerPortableClassDefinition() {
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(FACTORY_ID, InnerPortable.CLASS_ID);
        builder.addByteArrayField("b");
        builder.addCharArrayField("c");
        builder.addShortArrayField("s");
        builder.addIntArrayField("i");
        builder.addLongArrayField("l");
        builder.addFloatArrayField("f");
        builder.addDoubleArrayField("d");
        builder.addPortableArrayField("nn", createNamedPortableClassDefinition());
        return builder.build();
    }

    @Test
    public void testPacket_writeAndRead() {
        SerializationService ss = createSerializationService(10);

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("test", "pass");
        ClientPrincipal principal = new ClientPrincipal("uuid", "uuid2");
        Data data = ss.toData(new AuthenticationRequest(credentials, principal));

        Packet packet = new Packet(data, ss.getPortableContext());
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        assertTrue(packet.writeTo(buffer));

        SerializationService ss2 = createSerializationService(1);

        buffer.flip();
        packet = new Packet(ss2.getPortableContext());
        assertTrue(packet.readFrom(buffer));

        AuthenticationRequest request = ss2.toObject(packet.getData());
    }
}
