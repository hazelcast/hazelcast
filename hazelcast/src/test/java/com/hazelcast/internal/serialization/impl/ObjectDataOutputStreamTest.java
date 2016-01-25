package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationV1Dataserializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ObjectDataOutputStreamTest {

    private SerializationService mockSerializationService;
    private ObjectDataOutputStream dataOutputStream;
    private OutputStream mockOutputStream;
    private SerializationService serializationService;

    @Before
    public void before() throws Exception {
        DefaultSerializationServiceBuilder defaultSerializationServiceBuilder = new DefaultSerializationServiceBuilder();
        serializationService = defaultSerializationServiceBuilder.setVersion(SerializationService.VERSION_1).build();

        mockSerializationService = mock(SerializationService.class);
        Mockito.when(mockSerializationService.getByteOrder()).thenReturn(serializationService.getByteOrder());

        mockOutputStream = mock(OutputStream.class);
        dataOutputStream = SerializationUtil.createObjectDataOutputStream(mockOutputStream, mockSerializationService);
    }

    @Test
    public void testSampleEncodeDecode() throws IOException {
        SerializationV1Dataserializable testData = SerializationV1Dataserializable.createInstanceWithNonNullFields();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(1000);
        ObjectDataOutputStream output = SerializationUtil.createObjectDataOutputStream(outputStream, serializationService);

        testData.writeData(output);
        output.flush();

        byte[] buf = outputStream.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);
        ObjectDataInputStream input = SerializationUtil.createObjectDataInputStream(inputStream, serializationService);

        SerializationV1Dataserializable testDataFromSerializer = new SerializationV1Dataserializable();
        testDataFromSerializer.readData(input);

        assertTrue(testData.equals(testDataFromSerializer));
    }

    @Test
    public void testSampleEncodeDecode_with_null_arrays() throws IOException {
        SerializationV1Dataserializable testData = new SerializationV1Dataserializable();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(1000);
        ObjectDataOutputStream output = SerializationUtil.createObjectDataOutputStream(outputStream, serializationService);

        testData.writeData(output);
        output.flush();

        byte[] buf = outputStream.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);
        ObjectDataInputStream input = SerializationUtil.createObjectDataInputStream(inputStream, serializationService);

        SerializationV1Dataserializable testDataFromSerializer = new SerializationV1Dataserializable();
        testDataFromSerializer.readData(input);

        assertTrue(testData.equals(testDataFromSerializer));
    }

    @Test
    public void testWriteB() throws Exception {
        dataOutputStream.write(1);
        verify(mockOutputStream).write(1);
    }

    @Test
    public void testWriteForBOffLen() throws Exception {
        byte[] someInput = new byte[1];
        dataOutputStream.write(someInput, 0, someInput.length);
        verify(mockOutputStream).write(someInput, 0, someInput.length);
    }

    @Test
    public void testWriteObject() throws Exception {
        dataOutputStream.writeObject("INPUT");
        verify(mockSerializationService).writeObject(dataOutputStream, "INPUT");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testToByteArray() throws Exception {
        dataOutputStream.toByteArray();
    }

    @Test
    public void testFlush() throws Exception {
        dataOutputStream.flush();
        verify(mockOutputStream).flush();
    }

    @Test
    public void testClose() throws Exception {
        dataOutputStream.close();
        verify(mockOutputStream).close();
    }

    @Test
    public void testGetByteOrder() throws Exception {
        ByteOrder byteOrderActual = dataOutputStream.getByteOrder();
        assertEquals(serializationService.getByteOrder(), byteOrderActual);
    }

}
