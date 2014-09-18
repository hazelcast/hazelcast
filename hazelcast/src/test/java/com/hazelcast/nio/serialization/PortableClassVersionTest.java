package com.hazelcast.nio.serialization;

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.nio.serialization.PortableTest.createNamedPortableClassDefinition;
import static com.hazelcast.nio.serialization.PortableTest.createSerializationService;
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

    static final int FACTORY_ID = 1;

    @Test
    public void testDifferentClassVersions() {
        SerializationService serializationService = new SerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        SerializationService serializationService2 = new SerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        testDifferentClassVersions(serializationService, serializationService2);
    }

    @Test
    public void testDifferentClassAndServiceVersions() {
        SerializationService serializationService = new SerializationServiceBuilder().setVersion(1)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        SerializationService serializationService2 = new SerializationServiceBuilder().setVersion(2)
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

        NamedPortableV2 o1 = serializationService2.toObject(data);
        NamedPortable o2 = serializationService.toObject(data2);
        assertEquals(o1.name, o2.name);
    }

    @Test
    public void testDifferentClassVersionsUsingDataWriteAndRead() throws IOException {
        SerializationService serializationService = new SerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        SerializationService serializationService2 = new SerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        testDifferentClassVersionsUsingDataWriteAndRead(serializationService, serializationService2);
    }

    @Test
    public void testDifferentClassAndServiceVersionsUsingDataWriteAndRead() throws IOException {
        SerializationService serializationService = new SerializationServiceBuilder().setVersion(1)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        SerializationService serializationService2 = new SerializationServiceBuilder().setVersion(2)
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

        // emulate socket write by writing data to stream
        BufferObjectDataOutput out = serializationService.createObjectDataOutput(1024);
        data.writeData(out);
        byte[] bytes = out.toByteArray();

        // emulate socket read by reading data from stream
        BufferObjectDataInput in = serializationService2.createObjectDataInput(bytes);
        data = new Data();
        data.readData(in);

        // register class def and read data
        Object object1 = serializationService2.toObject(data);

        // serialize new portable version
        NamedPortableV2 p2 = new NamedPortableV2("portable-v2", 123);
        Data data2 = serializationService2.toData(p2);

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
        final Data data2 = serializationService.toData(mainPortable);
        assertEquals(mainPortable, serializationService2.toObject(data2));
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
}
