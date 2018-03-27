/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataInputStream;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataOutputStream;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.nio.IOUtil.compress;
import static com.hazelcast.nio.IOUtil.copy;
import static com.hazelcast.nio.IOUtil.copyFile;
import static com.hazelcast.nio.IOUtil.decompress;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.deleteQuietly;
import static com.hazelcast.nio.IOUtil.getFileFromResources;
import static com.hazelcast.nio.IOUtil.newInputStream;
import static com.hazelcast.nio.IOUtil.newOutputStream;
import static com.hazelcast.nio.IOUtil.readByteArray;
import static com.hazelcast.nio.IOUtil.readFully;
import static com.hazelcast.nio.IOUtil.readFullyOrNothing;
import static com.hazelcast.nio.IOUtil.readObject;
import static com.hazelcast.nio.IOUtil.toFileName;
import static com.hazelcast.nio.IOUtil.writeByteArray;
import static com.hazelcast.nio.IOUtil.writeObject;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IOUtilTest extends HazelcastTestSupport {

    private static final byte[] NON_EMPTY_BYTE_ARRAY = new byte[100];
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final int SIZE = 3;

    private static final InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testConstructor() {
        assertUtilityConstructor(IOUtil.class);
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

    private static byte[] writeAndReadByteArray(byte[] bytes) throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectDataOutput out = createObjectDataOutputStream(bout, serializationService);
        writeByteArray(out, bytes);
        byte[] data = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(data);
        ObjectDataInput in = createObjectDataInputStream(bin, serializationService);
        return readByteArray(in);
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

    private static Object writeAndReadObject(Object input) throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectDataOutput out = createObjectDataOutputStream(bout, serializationService);
        writeObject(out, input);
        byte[] data = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(data);
        ObjectDataInput in = createObjectDataInputStream(bin, serializationService);
        return readObject(in);
    }

    private final byte[] streamInput = {1, 2, 3, 4};

    @Test
    public void testReadFullyOrNothing() throws Exception {
        InputStream in = new ByteArrayInputStream(streamInput);
        byte[] buffer = new byte[4];

        boolean result = readFullyOrNothing(in, buffer);

        assertTrue(result);
        for (int i = 0; i < buffer.length; i++) {
            assertEquals(buffer[i], streamInput[i]);
        }
    }

    @Test
    public void testReadFullyOrNothing_whenThereIsNoData_thenReturnFalse() throws Exception {
        InputStream in = new ByteArrayInputStream(new byte[0]);
        byte[] buffer = new byte[4];

        boolean result = readFullyOrNothing(in, buffer);

        assertFalse(result);
    }

    @Test(expected = EOFException.class)
    public void testReadFullyOrNothing_whenThereIsNotEnoughData_thenThrowException() throws Exception {
        InputStream in = new ByteArrayInputStream(streamInput);
        byte[] buffer = new byte[8];

        readFullyOrNothing(in, buffer);
    }

    @Test
    public void testReadFully() throws Exception {
        InputStream in = new ByteArrayInputStream(streamInput);
        byte[] buffer = new byte[4];

        readFully(in, buffer);

        for (int i = 0; i < buffer.length; i++) {
            assertEquals(buffer[i], streamInput[i]);
        }
    }

    @Test(expected = EOFException.class)
    public void testReadFully_whenThereIsNoData_thenThrowException() throws Exception {
        InputStream in = new ByteArrayInputStream(new byte[0]);
        byte[] buffer = new byte[4];

        readFully(in, buffer);
    }

    @Test(expected = EOFException.class)
    public void testReadFully_whenThereIsNotEnoughData_thenThrowException() throws Exception {
        InputStream in = new ByteArrayInputStream(streamInput);
        byte[] buffer = new byte[8];

        readFully(in, buffer);
    }

    @Test
    public void testNewOutputStream_shouldWriteWholeByteBuffer() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        OutputStream outputStream = newOutputStream(buffer);
        assertEquals(SIZE, buffer.remaining());

        outputStream.write(new byte[SIZE]);

        assertEquals(0, buffer.remaining());
    }

    @Test
    public void testNewOutputStream_shouldWriteSingleByte() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        OutputStream outputStream = newOutputStream(buffer);
        assertEquals(SIZE, buffer.remaining());

        outputStream.write(23);

        assertEquals(SIZE - 1, buffer.remaining());
    }

    @Test
    public void testNewOutputStream_shouldWriteInChunks() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        OutputStream outputStream = newOutputStream(buffer);
        assertEquals(SIZE, buffer.remaining());

        outputStream.write(new byte[1], 0, 1);
        outputStream.write(new byte[SIZE - 1], 0, SIZE - 1);

        assertEquals(0, buffer.remaining());
    }

    @Test(expected = BufferOverflowException.class)
    public void testNewOutputStream_shouldThrowWhenTryingToWriteToEmptyByteBuffer() throws Exception {
        ByteBuffer empty = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
        OutputStream outputStream = newOutputStream(empty);

        outputStream.write(23);
    }

    @Test
    public void testNewInputStream_shouldReturnMinusOneWhenEmptyByteBufferProvidedAndReadingOneByte() throws Exception {
        ByteBuffer empty = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
        InputStream inputStream = newInputStream(empty);

        int read = inputStream.read();

        assertEquals(-1, read);
    }

    @Test
    public void testNewInputStream_shouldReadWholeByteBuffer() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        InputStream inputStream = newInputStream(buffer);

        int read = inputStream.read(new byte[SIZE]);

        assertEquals(SIZE, read);
    }

    @Test
    public void testNewInputStream_shouldAllowReadingByteBufferInChunks() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        InputStream inputStream = newInputStream(buffer);

        int firstRead = inputStream.read(new byte[1]);
        int secondRead = inputStream.read(new byte[SIZE - 1]);

        assertEquals(1, firstRead);
        assertEquals(SIZE - 1, secondRead);
    }

    @Test
    public void testNewInputStream_shouldReturnMinusOneWhenNothingRemainingInByteBuffer() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        InputStream inputStream = newInputStream(buffer);

        int firstRead = inputStream.read(new byte[SIZE]);
        int secondRead = inputStream.read();

        assertEquals(SIZE, firstRead);
        assertEquals(-1, secondRead);
    }

    @Test
    public void testNewInputStream_shouldReturnMinusOneWhenEmptyByteBufferProvidedAndReadingSeveralBytes() throws Exception {
        ByteBuffer empty = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
        InputStream inputStream = newInputStream(empty);

        int read = inputStream.read(NON_EMPTY_BYTE_ARRAY);

        assertEquals(-1, read);
    }

    @Test(expected = EOFException.class)
    public void testNewInputStream_shouldThrowWhenTryingToReadFullyFromEmptyByteBuffer() throws Exception {
        ByteBuffer empty = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
        DataInputStream inputStream = new DataInputStream(newInputStream(empty));

        inputStream.readFully(NON_EMPTY_BYTE_ARRAY);
    }

    @Test(expected = EOFException.class)
    public void testNewInputStream_shouldThrowWhenByteBufferExhaustedAndTryingToReadFully() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        DataInputStream inputStream = new DataInputStream(newInputStream(buffer));
        inputStream.readFully(new byte[SIZE]);

        inputStream.readFully(NON_EMPTY_BYTE_ARRAY);
    }

    @Test
    public void testCompressAndDecompress() throws Exception {
        String expected = "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born"
                + " and I will give you a complete account of the system, and expound the actual teachings of the great explorer"
                + " of the truth, the master-builder of human happiness.";

        byte[] compressed = compress(expected.getBytes());
        byte[] decompressed = decompress(compressed);

        assertEquals(expected, new String(decompressed));
    }

    @Test
    public void testCompressAndDecompress_withEmptyInput() throws Exception {
        byte[] compressed = compress(EMPTY_BYTE_ARRAY);
        byte[] decompressed = decompress(compressed);

        assertArrayEquals(EMPTY_BYTE_ARRAY, decompressed);
    }

    @Test
    public void testCompressAndDecompress_withSingleByte() throws Exception {
        byte[] input = new byte[] {111};

        byte[] compressed = compress(input);
        byte[] decompressed = decompress(compressed);

        assertArrayEquals(input, decompressed);
    }

    @Test
    public void testCloseResource() throws Exception {
        Closeable closeable = mock(Closeable.class);

        closeResource(closeable);

        verify(closeable).close();
        verifyNoMoreInteractions(closeable);
    }

    @Test
    public void testCloseResource_withException() throws Exception {
        Closeable closeable = mock(Closeable.class);
        doThrow(new IOException("expected")).when(closeable).close();

        closeResource(closeable);

        verify(closeable).close();
        verifyNoMoreInteractions(closeable);
    }

    @Test
    public void testCloseResource_withNull() {
        closeResource(null);
    }

    @Test
    public void testDelete_shouldDoNothingWithNonExistentFile() {
        File file = new File("notFound");

        delete(file);
    }

    @Test
    public void testDelete_shouldDeleteDirectoryRecursively() throws Exception {
        File parentDir = createDirectory("parent");
        File file1 = createFile(parentDir, "file1");
        File file2 = createFile(parentDir, "file2");
        File childDir = createDirectory(parentDir, "child");
        File childFile1 = createFile(childDir, "childFile1");
        File childFile2 = createFile(childDir, "childFile2");

        delete(parentDir);

        assertFalse(parentDir.exists());
        assertFalse(file1.exists());
        assertFalse(file2.exists());
        assertFalse(childDir.exists());
        assertFalse(childFile1.exists());
        assertFalse(childFile2.exists());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCopyFailsWhenSourceDoesntExist() {
        copy(new File("nonExistant"), new File("target"));
    }

    @Test
    public void testCopyFileFailsWhenTargetDoesntExistAndCannotBeCreated() throws IOException {
        final File target = mock(File.class);
        when(target.exists()).thenReturn(false);
        when(target.mkdirs()).thenReturn(false);
        final File source = new File("source");
        assertTrue(!source.exists());
        source.createNewFile();

        try {
            copyFile(source, target, -1);
            fail();
        } catch (HazelcastException expected) {
            ignore(expected);
        }

        delete(source);
    }

    @Test
    public void testCopyFailsWhenSourceCannotBeListed() {
        final File source = mock(File.class);
        when(source.exists()).thenReturn(true);
        when(source.isDirectory()).thenReturn(true);
        when(source.listFiles()).thenReturn(null);
        when(source.getName()).thenReturn("dummy");

        final File dest = new File("dest");
        assertTrue(!dest.exists());
        dest.mkdir();

        try {
            copy(source, dest);
            fail();
        } catch (HazelcastException expected) {
            ignore(expected);
        }

        delete(dest);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCopyFileFailsWhenSourceDoesntExist() {
        copyFile(new File("nonExistant"), new File("target"), -1);
    }

    @Test
    public void testCopyFileFailsWhenSourceIsNotAFile() {
        final File source = new File("source");
        assertTrue(!source.exists());
        source.mkdirs();
        try {
            copyFile(source, new File("target"), -1);
            fail();
        } catch (IllegalArgumentException expected) {
            ignore(expected);
        }
        delete(source);
    }

    @Test
    public void testCopyFailsWhenSourceIsDirAndTargetIsFile() throws IOException {
        final File source = new File("dir1");
        final File target = new File("file1");
        assertTrue(!source.exists() && !target.exists());
        source.mkdir();
        target.createNewFile();
        try {
            copy(source, target);
            fail();
        } catch (IllegalArgumentException expected) {
            ignore(expected);
        }
        delete(source);
        delete(target);
    }

    @Test
    public void testCopyRecursiveDirectory() {
        final File dir = new File("dir");
        final File subdir = new File(dir, "subdir");
        final File f1 = new File(dir, "f1");
        final File f2 = new File(subdir, "f2");
        assertTrue(!dir.exists());
        assertTrue(!subdir.exists());

        dir.mkdir();
        subdir.mkdir();
        writeTo(f1, "testContent");
        writeTo(f2, "otherContent");

        final File copy = new File("copy");
        assertTrue(!copy.exists());

        copy(dir, copy);
        assertTrue(copy.exists());
        assertEqualFiles(dir, new File(copy, "dir"));

        delete(dir);
        delete(subdir);
        delete(copy);
    }

    @Test
    public void testDelete_shouldDeleteSingleFile() throws Exception {
        File file = createFile("singleFile");

        delete(file);

        assertFalse(file.exists());
    }

    @Test(expected = HazelcastException.class)
    public void testDelete_shouldThrowIfFileCouldNotBeDeleted() {
        File file = mock(File.class);
        when(file.exists()).thenReturn(true);
        when(file.delete()).thenReturn(false);

        delete(file);
    }

    @Test
    public void testDeleteQuietly_shouldDeleteSingleFile() throws Exception {
        File file = createFile("singleFile");

        deleteQuietly(file);

        assertFalse(file.exists());
    }

    @Test
    public void testDeleteQuietly_shouldDoNothingIfFileCouldNotBeDeleted() {
        File file = mock(File.class);
        when(file.exists()).thenReturn(true);
        when(file.delete()).thenReturn(false);

        deleteQuietly(file);
    }

    private static File createDirectory(String dirName) {
        File dir = new File(dirName);
        return createDirectory(dir);
    }

    private static File createDirectory(File parent, String dirName) {
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

        String actual = toFileName(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void testToFileName_shouldChangeInvalidFileName() {
        String expected = "a_b_c_d_e_f_g_h_j_k_l_m.txt";

        String actual = toFileName("a:b?c*d\"e|f<g>h'j,k\\l/m.txt");

        assertEquals(expected, actual);
    }

    @Test
    public void testGetFileFromResources_shouldReturnExistingFile() {
        File file = getFileFromResources("logging.properties");

        assertTrue(file.exists());
    }

    @Test(expected = HazelcastException.class)
    public void testGetFileFromResources_shouldThrowExceptionIfFileDoesNotExist() {
        getFileFromResources("doesNotExist");
    }

    private static void writeTo(File f1, String testContent) {
        FileWriter w = null;
        try {
            w = new FileWriter(f1);
            w.write(testContent);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeResource(w);
        }
    }

    private static void assertEqualFiles(File f1, File f2) {
        if (f1.exists()) {
            assertTrue(f2.exists());
        }
        assertTrue(f1.getName().equals(f2.getName()));
        if (f1.isFile()) {
            assertTrue(f2.isFile());
            if (!equalContents(f1, f2)) {
                fail();
            }
            return;
        }
        final File[] f1Files = f1.listFiles();
        assertTrue(f1Files.length == f2.listFiles().length);
        for (File f : f1Files) {
            assertEqualFiles(f, new File(f2, f.getName()));
        }
    }

    // note: use only for small files (e.g. up to a couple of hundred KBs). See below.
    private static boolean equalContents(File f1, File f2) {
        InputStream is1 = null;
        InputStream is2 = null;
        try {
            is1 = new FileInputStream(f1);
            is2 = new FileInputStream(f2);
            // compare byte-by-byte since InputStream.read(byte[]) possibly doesn't return the requested number of bytes
            // this is why this method should be used for smallFiles
            int data;
            while ((data = is1.read()) != -1) {
                if (data != is2.read()) {
                    return false;
                }
            }
            if (is2.read() != -1) {
                return false;
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            closeResource(is1);
            closeResource(is2);
        }
        return true;
    }
}
