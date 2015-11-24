package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.util.Random;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ObjectDataInputStreamNonFinalMethodsTest {

    final static byte[] INIT_DATA = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    private SerializationService mockSerializationService;
    private ObjectDataInputStream in;
    private DataInputStream dataInputSpy;
    private ByteArrayInputStream inputStream;
    private ByteOrder byteOrder;

    @Before
    public void before() throws Exception {
        byteOrder = BIG_ENDIAN;
        mockSerializationService = mock(SerializationService.class);
        when(mockSerializationService.getByteOrder()).thenReturn(byteOrder);

        inputStream = new ByteArrayInputStream(INIT_DATA);
        in = new ObjectDataInputStream(inputStream, mockSerializationService);

        Field field = ObjectDataInputStream.class.getDeclaredField("dataInput");
        field.setAccessible(true);
        DataInputStream dataInput = (DataInputStream) field.get(in);

        dataInputSpy = spy(dataInput);

        field.set(in, dataInputSpy);
    }

    @Test
    public void testSkip() throws Exception {
        long someInput = new Random().nextLong();
        in.skip(someInput);
        verify(dataInputSpy).skip(someInput);
    }

    @Test
    public void testAvailable() throws Exception {
        in.available();
        verify(dataInputSpy).available();
    }

    @Test
    public void testClose() throws Exception {
        in.close();
        verify(dataInputSpy).close();
    }

    @Test
    public void testMark() throws Exception {
        int someInput = new Random().nextInt();
        in.mark(someInput);
        verify(dataInputSpy).mark(someInput);
    }

    @Test
    public void testReset() throws Exception {
        in.reset();
        verify(dataInputSpy).reset();
    }

    @Test
    public void testMarkSupported() throws Exception {
        in.markSupported();
        verify(dataInputSpy).markSupported();
    }
}
