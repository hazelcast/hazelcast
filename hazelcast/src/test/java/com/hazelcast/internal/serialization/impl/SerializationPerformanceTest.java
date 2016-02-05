package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.impl.bufferpool.BufferPoolThreadLocal;
import com.hazelcast.nio.BufferObjectDataOutput;


import com.hazelcast.internal.serialization.impl.bufferpool.BufferPool;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteOrder;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.handleException;

public class SerializationPerformanceTest {

    private AbstractSerializationService serializationService;
    private int iterations = 500 * 1000 * 1000;
    private SerializerAdapter serializer;
    private BufferPool pool;

    @Before
    public void setup() {
        serializationService = (AbstractSerializationService) new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(1, new DataSerializableFactory() {
                    @Override
                    public IdentifiedDataSerializable create(int typeId) {
                        return new IdentifiedDataSerializableValue();
                    }
                })
                .build();

//        serializer = serializationService.serializerFor(new IdentifiedDataSerializableValue());
//        pool = serializationService.bufferPoolThreadLocal.get();
    }
//
//    @Test
//    public void testSerialization() {
//        SerializableValue value = new SerializableValue();
//
//        long startMs = System.currentTimeMillis();
//        for (int k = 0; k < iterations; k++) {
//            Data data = serializationService.toData(value);
//            serializationService.toObject(data);
//        }
//        long durationMs = System.currentTimeMillis() - startMs;
//        double performance = (1000.0 * iterations) / durationMs;
//        System.out.println("Performance: " + performance + " serializations/second");
//    }
//
//    @Test
//    public void testExternalizable() {
//        ExternalizableValue value = new ExternalizableValue();
//
//        long startMs = System.currentTimeMillis();
//        for (int k = 0; k < iterations; k++) {
//            Data data = serializationService.toData(value);
//            serializationService.toObject(data);
//        }
//        long durationMs = System.currentTimeMillis() - startMs;
//        double performance = (1000.0 * iterations) / durationMs;
//        System.out.println("Performance: " + performance + " serializations/second");
//    }
//
//    @Test
//    public void testDataSerializable() {
//        DataSerializableValue value = new DataSerializableValue();
//
//        long startMs = System.currentTimeMillis();
//        for (int k = 0; k < iterations; k++) {
//            Data data = serializationService.toData(value);
//            serializationService.toObject(data);
//        }
//        long durationMs = System.currentTimeMillis() - startMs;
//        double performance = (1000.0 * iterations) / durationMs;
//        System.out.println("Performance: " + performance + " serializations/second");
//    }
//
//    @Test
//    public void testIdentifiedDataSerializale() {
//        IdentifiedDataSerializableValue value = new IdentifiedDataSerializableValue();
//
//        long startMs = System.currentTimeMillis();
//        for (int k = 0; k < iterations; k++) {
//            Data data = serializationService.toData(value);
//            serializationService.toObject(data);
//        }
//        long durationMs = System.currentTimeMillis() - startMs;
//        double performance = (1000.0 * iterations) / durationMs;
//        System.out.println("Performance: " + performance + " serializations/second");
//    }

    @Test
    public void testLong() {
        Long value = new Long(234533455);

        long startMs = System.currentTimeMillis();
        for (int k = 0; k < iterations; k++) {
            Data data = serializationService.toData(value);
            serializationService.toObject(data);
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (1000.0 * iterations) / durationMs;
        System.out.println("Performance: " + performance + " serializations/second");
    }

    @Test
    public void testLong_Serialization() {
        Long value = new Long(1);

        long startMs = System.currentTimeMillis();
        for (int k = 0; k < iterations; k++) {
            Data data = serializationService.toData(value);
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (1000.0 * iterations) / durationMs;
        System.out.println("Performance: " + performance + " serializations/second");
    }

    @Test
    public void testLong_Deserialization() {
        Long value = new Long(1);

        long startMs = System.currentTimeMillis();
        Data data = serializationService.toData(value);
        for (int k = 0; k < iterations; k++) {
            serializationService.toObject(data);
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (1000.0 * iterations) / durationMs;
        System.out.println("Performance: " + performance + " serializations/second");
    }

    @Test
    public void testLong_Deserialization_optimizedBufferPool() {
        Long value = new Long(1);

        BufferPool bufferPool = serializationService.bufferPoolThreadLocal.get();

        long startMs = System.currentTimeMillis();
        Data data = serializationService.toData(value);
        for (int k = 0; k < iterations; k++) {
            serializationService.toObject(data, bufferPool);
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (1000.0 * iterations) / durationMs;
        System.out.println("Performance: " + performance + " serializations/second");
    }

    @Test
    public void testByte() {
        Byte value = new Byte((byte)123);

        long startMs = System.currentTimeMillis();
        for (int k = 0; k < iterations; k++) {
            Data data = serializationService.toData(value);
           // serializationService.toObject(data);
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (1000.0 * iterations) / durationMs;
        System.out.println("Performance: " + performance + " serializations/second");
    }

    @Test
    public void testAInteger() {
        Integer value = new Integer(234533455);

        long startMs = System.currentTimeMillis();
        for (int k = 0; k < iterations; k++) {
            Data data = serializationService.toData(value);
         //   serializationService.toObject(data);
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (1000.0 * iterations) / durationMs;
        System.out.println("Performance: " + performance + " serializations/second");
    }

    @Test
    public void testFloat() {
        Float value = new Float(234533455);

        long startMs = System.currentTimeMillis();
        for (int k = 0; k < iterations; k++) {
            Data data = serializationService.toData(value);
          //  serializationService.toObject(data);
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (1000.0 * iterations) / durationMs;
        System.out.println("Performance: " + performance + " serializations/second");
    }


    @Test
    public void testIdentifiedDataSerializableFast() {
        IdentifiedDataSerializableValue value = new IdentifiedDataSerializableValue(1);

        long startMs = System.currentTimeMillis();
        for (int k = 0; k < iterations; k++) {
            Data data = serializationService.toData(value);
          //  serializationService.toObject(data);
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (1000.0 * iterations) / durationMs;
        System.out.println("Performance: " + performance + " serializations/second");
    }

    private final static class IdentifiedDataSerializableValue implements IdentifiedDataSerializable{
        private int value;

        public IdentifiedDataSerializableValue(){}

        public IdentifiedDataSerializableValue(int value){
            this.value = value;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getId() {
            return 1;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            value = in.readInt();
        }
    }


//    public final <T> T toObject(final Data data) {
//        //   BufferPool pool = serializationService.bufferPoolThreadLocal.get();
//        BufferObjectDataInput in = pool.takeInputBuffer(data);
//        try {
//            int typeId = data.getType();
//            //     serializer = serializationService.serializerFor(typeId);
//            Object obj = serializer.read(in);
//            return (T) obj;
//        } catch (Throwable e) {
//            throw handleException(e);
//        } finally {
//            pool.returnInputBuffer(in);
//        }
//    }
//
//    public final Data toData(final Object obj) {
//        // SerializerAdapter serializer = serializationService.serializerFor(obj);
//        // BufferPool pool = serializationService.bufferPoolThreadLocal.get();
//        BufferObjectDataOutput out = pool.takeOutputBuffer();
//        try {
//            // int partitionHash = serializationService.calculatePartitionHash(obj, null);
//            out.writeInt(0, ByteOrder.BIG_ENDIAN);
//
//            out.writeInt(serializer.getTypeId(), ByteOrder.BIG_ENDIAN);
//
//            serializer.write(out, obj);
//            return new HeapData(out.toByteArray());
//        } catch (Throwable e) {
//            throw handleException(e);
//        } finally {
//            pool.returnOutputBuffer(out);
//        }
//    }
}
