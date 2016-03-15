/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.test.AbstractTestOperation;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataInputStream;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataOutputStream;
import static com.hazelcast.nio.IOUtil.extractOperationCallId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Tomasz Nurkiewicz
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IOUtilTest extends HazelcastTestSupport {

    private static final byte[] NON_EMPTY_BYTE_ARRAY = new byte[100];
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final int SIZE = 3;

    private static TestHazelcastInstanceFactory hazelcastInstanceFactory;
    private static InternalSerializationService serializationService;

    @BeforeClass
    public static void setUp() {
        hazelcastInstanceFactory = new TestHazelcastInstanceFactory();

        HazelcastInstance hazelcastInstance = hazelcastInstanceFactory.newHazelcastInstance();
        serializationService = getSerializationService(hazelcastInstance);
    }

    @AfterClass
    public static void tearDown() {
        hazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(IOUtil.class);
    }

    @Test
    public void testExtractOperationCallId() throws Exception {
        IoUtilTestOperation operation = new IoUtilTestOperation(1);
        OperationAccessor.setCallId(operation, 2342);
        Data data = serializationService.toData(operation);

        long callId = extractOperationCallId(data, serializationService);

        assertEquals(2342, callId);
    }

    @Test
    public void testExtractOperationCallId_withIdentifiedOperation() throws Exception {
        IdentifiedIoUtilTestOperation operation = new IdentifiedIoUtilTestOperation(1);
        OperationAccessor.setCallId(operation, 4223);
        Data data = serializationService.toData(operation);

        long callId = extractOperationCallId(data, serializationService);

        assertEquals(4223, callId);
    }

    @Test
    public void testWriteAndReadByteArray() throws Exception {
        byte[] bytes = new byte[SIZE];
        bytes[0] = SIZE - 1;
        bytes[1] = 23;
        bytes[2] = 42;

        byte[] output = writeAndReadByteArray(bytes);

        assertNotNull(output);
        assertEquals(SIZE - 1, output[0]);
        assertEquals(23, output[1]);
        assertEquals(42, output[2]);
    }

    @Test
    public void testWriteAndReadByteArray_withNull() throws Exception {
        byte[] output = writeAndReadByteArray(null);

        assertNull(output);
    }

    private static byte[] writeAndReadByteArray(byte[] bytes) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectDataOutput out = createObjectDataOutputStream(bout, serializationService);
        IOUtil.writeByteArray(out, bytes);
        byte[] data = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(data);
        ObjectDataInput in = createObjectDataInputStream(bin, serializationService);
        return IOUtil.readByteArray(in);
    }

    @Test
    public void testWriteAndReadObject() throws Exception {
        String expected = "test input";

        String actual = (String) writeAndReadObject(expected);

        assertNotNull(actual);
        assertEquals(expected, actual);
    }

    @Test
    public void testWriteAndReadObject_withData() throws Exception {
        Data expected = serializationService.toData("test input");

        Data actual = (Data) writeAndReadObject(expected);

        assertNotNull(actual);
        assertEquals(expected, actual);
    }

    private static Object writeAndReadObject(Object input) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectDataOutput out = createObjectDataOutputStream(bout, serializationService);
        IOUtil.writeObject(out, input);
        byte[] data = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(data);
        ObjectDataInput in = createObjectDataInputStream(bin, serializationService);
        return IOUtil.readObject(in);
    }

    @Test
    public void testNewOutputStream_shouldWriteWholeByteBuffer() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        OutputStream outputStream = IOUtil.newOutputStream(buffer);
        assertEquals(SIZE, buffer.remaining());

        outputStream.write(new byte[SIZE]);

        assertEquals(0, buffer.remaining());
    }

    @Test
    public void testNewOutputStream_shouldWriteSingleByte() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        OutputStream outputStream = IOUtil.newOutputStream(buffer);
        assertEquals(SIZE, buffer.remaining());

        outputStream.write(23);

        assertEquals(SIZE - 1, buffer.remaining());
    }

    @Test
    public void testNewOutputStream_shouldWriteInChunks() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        OutputStream outputStream = IOUtil.newOutputStream(buffer);
        assertEquals(SIZE, buffer.remaining());

        outputStream.write(new byte[1], 0, 1);
        outputStream.write(new byte[SIZE - 1], 0, SIZE - 1);

        assertEquals(0, buffer.remaining());
    }

    @Test(expected = BufferOverflowException.class)
    public void testNewOutputStream_shouldThrowWhenTryingToWriteToEmptyByteBuffer() throws Exception {
        ByteBuffer empty = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
        OutputStream outputStream = IOUtil.newOutputStream(empty);

        outputStream.write(23);
    }

    @Test
    public void testNewInputStream_shouldReturnMinusOneWhenEmptyByteBufferProvidedAndReadingOneByte() throws Exception {
        ByteBuffer empty = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
        InputStream inputStream = IOUtil.newInputStream(empty);

        int read = inputStream.read();

        assertEquals(-1, read);
    }

    @Test
    public void testNewInputStream_shouldReadWholeByteBuffer() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        InputStream inputStream = IOUtil.newInputStream(buffer);

        int read = inputStream.read(new byte[SIZE]);

        assertEquals(SIZE, read);
    }

    @Test
    public void testNewInputStream_shouldAllowReadingByteBufferInChunks() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        InputStream inputStream = IOUtil.newInputStream(buffer);

        int firstRead = inputStream.read(new byte[1]);
        int secondRead = inputStream.read(new byte[SIZE - 1]);

        assertEquals(1, firstRead);
        assertEquals(SIZE - 1, secondRead);
    }

    @Test
    public void testNewInputStream_shouldReturnMinusOneWhenNothingRemainingInByteBuffer() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        InputStream inputStream = IOUtil.newInputStream(buffer);

        int firstRead = inputStream.read(new byte[SIZE]);
        int secondRead = inputStream.read();

        assertEquals(SIZE, firstRead);
        assertEquals(-1, secondRead);
    }

    @Test
    public void testNewInputStream_shouldReturnMinusOneWhenEmptyByteBufferProvidedAndReadingSeveralBytes() throws Exception {
        ByteBuffer empty = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
        InputStream inputStream = IOUtil.newInputStream(empty);

        int read = inputStream.read(NON_EMPTY_BYTE_ARRAY);

        assertEquals(-1, read);
    }

    @Test(expected = EOFException.class)
    public void testNewInputStream_shouldThrowWhenTryingToReadFullyFromEmptyByteBuffer() throws Exception {
        ByteBuffer empty = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
        DataInputStream inputStream = new DataInputStream(IOUtil.newInputStream(empty));

        inputStream.readFully(NON_EMPTY_BYTE_ARRAY);
    }

    @Test(expected = EOFException.class)
    public void testNewInputStream_shouldThrowWhenByteBufferExhaustedAndTryingToReadFully() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        DataInputStream inputStream = new DataInputStream(IOUtil.newInputStream(buffer));
        inputStream.readFully(new byte[SIZE]);

        inputStream.readFully(NON_EMPTY_BYTE_ARRAY);
    }

    @Test
    public void testCompressAndDecompress() throws Exception {
        String expected = "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born"
                + " and I will give you a complete account of the system, and expound the actual teachings of the great explorer"
                + " of the truth, the master-builder of human happiness.";

        byte[] compressed = IOUtil.compress(expected.getBytes());
        byte[] decompressed = IOUtil.decompress(compressed);

        assertEquals(expected, new String(decompressed));
    }

    @Test
    public void testCompressAndDecompress_withEmptyString() throws Exception {
        String expected = "";

        byte[] compressed = IOUtil.compress(expected.getBytes());
        byte[] decompressed = IOUtil.decompress(compressed);

        assertEquals(expected, new String(decompressed));
    }

    @Test
    public void testCloseResource() throws Exception {
        Closeable closeable = mock(Closeable.class);

        IOUtil.closeResource(closeable);

        verify(closeable).close();
        verifyNoMoreInteractions(closeable);
    }

    @Test
    public void testCloseResource_withException() throws Exception {
        Closeable closeable = mock(Closeable.class);
        doThrow(new IOException("expected")).when(closeable).close();

        IOUtil.closeResource(closeable);

        verify(closeable).close();
        verifyNoMoreInteractions(closeable);
    }

    @Test
    public void testCloseResource_withNull() {
        IOUtil.closeResource(null);
    }

    private static class IoUtilTestOperation extends AbstractTestOperation {

        public IoUtilTestOperation(int partitionId) {
            super(partitionId);
        }

        @Override
        protected Object doRun() {
            return null;
        }
    }

    @Test
    public void testDelete_shouldDoNothingWithNonExistentFile() {
        File file = new File("notFound");

        IOUtil.delete(file);
    }

    @Test
    public void testDelete_shouldDeleteDirectoryRecursively() throws Exception {
        File parentDir = createDirectory("parent");
        File file1 = createFile(parentDir, "file1");
        File file2 = createFile(parentDir, "file2");
        File childDir = createDirectory(parentDir, "child");
        File childFile1 = createFile(childDir, "childFile1");
        File childFile2 = createFile(childDir, "childFile2");

        IOUtil.delete(parentDir);

        assertFalse(parentDir.exists());
        assertFalse(file1.exists());
        assertFalse(file2.exists());
        assertFalse(childDir.exists());
        assertFalse(childFile1.exists());
        assertFalse(childFile2.exists());
    }

    @Test
    public void testDelete_shouldDeleteSingleFile() throws Exception {
        File file = createFile("singleFile");

        IOUtil.delete(file);

        assertFalse(file.exists());
    }

    @Test(expected = HazelcastException.class)
    public void testDelete_shouldThrowIfFileCouldNotBeDeleted() throws Exception {
        File file = mock(File.class);
        when(file.exists()).thenReturn(true);
        when(file.delete()).thenReturn(false);

        IOUtil.delete(file);
    }

    private static File createDirectory(String dirName) throws IOException {
        File dir = new File(dirName);
        return createDirectory(dir);
    }

    private static File createDirectory(File parent, String dirName) throws IOException {
        File dir = new File(parent, dirName);
        return createDirectory(dir);
    }

    private static File createDirectory(File dir) {
        if (dir.isDirectory()) {
            return dir;
        }
        if (!dir.mkdirs() || !dir.exists()) {
            fail("Could not create directory " + dir.getAbsolutePath());
        }
        return dir;
    }

    private static File createFile(String fileName) throws IOException {
        File file = new File(fileName);
        return createFile(file);
    }

    private static File createFile(File parent, String fileName) throws IOException {
        File file = new File(parent, fileName);
        return createFile(file);
    }

    private static File createFile(File file) throws IOException {
        if (file.isFile()) {
            return file;
        }
        if (!file.createNewFile() || !file.exists()) {
            fail("Could not create file " + file.getAbsolutePath());
        }
        return file;
    }

    @Test
    public void testToFileName_shouldNotChangeValidFileName() {
        String expected = "valid-fileName_23.txt";

        String actual = IOUtil.toFileName(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void testToFileName_shouldChangeInvalidFileName() {
        String expected = "a_b_c_d_e_f_g_h_j_k_l_m.txt";

        String actual = IOUtil.toFileName("a:b?c*d\"e|f<g>h'j,k\\l/m.txt");

        assertEquals(expected, actual);
    }

    private static class IdentifiedIoUtilTestOperation extends IoUtilTestOperation implements IdentifiedDataSerializable {

        public IdentifiedIoUtilTestOperation(int partitionId) {
            super(partitionId);
        }

        @Override
        public int getFactoryId() {
            return 23;
        }

        @Override
        public int getId() {
            return 42;
        }
    }
}
