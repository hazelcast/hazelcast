/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.nio.IOUtil.close;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.IOUtil.compress;
import static com.hazelcast.internal.nio.IOUtil.copy;
import static com.hazelcast.internal.nio.IOUtil.copyFile;
import static com.hazelcast.internal.nio.IOUtil.copyToHeapBuffer;
import static com.hazelcast.internal.nio.IOUtil.decompress;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static com.hazelcast.internal.nio.IOUtil.deleteQuietly;
import static com.hazelcast.internal.nio.IOUtil.getFileFromResources;
import static com.hazelcast.internal.nio.IOUtil.getPath;
import static com.hazelcast.internal.nio.IOUtil.newInputStream;
import static com.hazelcast.internal.nio.IOUtil.newOutputStream;
import static com.hazelcast.internal.nio.IOUtil.readFully;
import static com.hazelcast.internal.nio.IOUtil.readFullyOrNothing;
import static com.hazelcast.internal.nio.IOUtil.readObject;
import static com.hazelcast.internal.nio.IOUtil.rename;
import static com.hazelcast.internal.nio.IOUtil.toFileName;
import static com.hazelcast.internal.nio.IOUtil.touch;
import static com.hazelcast.internal.nio.IOUtil.writeObject;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataInputStream;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataOutputStream;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static java.lang.Integer.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class IOUtilTest extends HazelcastTestSupport {

    private static final int SIZE = 3;
    private static final byte[] NON_EMPTY_BYTE_ARRAY = new byte[100];
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final byte[] STREAM_INPUT = {1, 2, 3, 4};

    @Rule
    public TestName testName = new TestName();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private final InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    private final List<File> files = new ArrayList<>();

    @After
    public void tearDown() {
        for (File file : files) {
            deleteQuietly(file);
        }
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(IOUtil.class);
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

    private Object writeAndReadObject(Object input) throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectDataOutput out = createObjectDataOutputStream(bout, serializationService);
        writeObject(out, input);
        byte[] data = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(data);
        ObjectDataInput in = createObjectDataInputStream(bin, serializationService);
        return readObject(in);
    }

    @Test
    public void testReadFullyOrNothing() throws Exception {
        InputStream in = new ByteArrayInputStream(STREAM_INPUT);
        byte[] buffer = new byte[4];

        boolean result = readFullyOrNothing(in, buffer);

        assertTrue(result);
        for (int i = 0; i < buffer.length; i++) {
            assertEquals(buffer[i], STREAM_INPUT[i]);
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
        InputStream in = new ByteArrayInputStream(STREAM_INPUT);
        byte[] buffer = new byte[8];

        readFullyOrNothing(in, buffer);
    }

    @Test
    public void testReadFully() throws Exception {
        InputStream in = new ByteArrayInputStream(STREAM_INPUT);
        byte[] buffer = new byte[4];

        readFully(in, buffer);

        for (int i = 0; i < buffer.length; i++) {
            assertEquals(buffer[i], STREAM_INPUT[i]);
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
        InputStream in = new ByteArrayInputStream(STREAM_INPUT);
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
    public void testCompressAndDecompress() {
        String expected = "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born"
                + " and I will give you a complete account of the system, and expound the actual teachings of the great explorer"
                + " of the truth, the master-builder of human happiness.";

        byte[] compressed = compress(expected.getBytes());
        byte[] decompressed = decompress(compressed);

        assertEquals(expected, new String(decompressed));
    }

    @Test
    public void testCompressAndDecompress_withEmptyInput() {
        byte[] compressed = compress(EMPTY_BYTE_ARRAY);
        byte[] decompressed = decompress(compressed);

        assertArrayEquals(EMPTY_BYTE_ARRAY, decompressed);
    }

    @Test
    public void testCompressAndDecompress_withSingleByte() {
        byte[] input = new byte[]{111};

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
    public void testTouch() {
        File file = newFile("touchMe");
        assertFalse("file should not exist yet", file.exists());

        touch(file);

        assertTrue("file should exist now", file.exists());
    }

    @Test(expected = HazelcastException.class)
    public void testTouch_failsWhenLastModifiedCannotBeSet() {
        File file = new File(tempFolder.getRoot(), "touchMe") {
            @Override
            public boolean setLastModified(long time) {
                return false;
            }
        };

        touch(file);
    }

    @Test
    public void testCopy_withRecursiveDirectory() {
        File parentDir = newFile("parent");
        assertFalse("parentDir should not exist yet", parentDir.exists());
        assertTrue("parentDir should have been created", parentDir.mkdir());

        File childDir = newFile(parentDir, "child");
        assertFalse("childDir should not exist yet", childDir.exists());
        assertTrue("childDir folder should have been created", childDir.mkdir());

        File parentFile = newFile(parentDir, "parentFile");
        writeTo(parentFile, "parentContent");

        File childFile = newFile(childDir, "childFile");
        writeTo(childFile, "childContent");

        File target = newFile("target");
        assertFalse("target should not exist yet", target.exists());

        copy(parentDir, target);

        assertTrue("target should exist now", target.exists());
        assertEqualFiles(parentDir, new File(target, parentDir.getName()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCopy_failsWhenSourceNotExist() {
        copy(newFile("nonExistent"), newFile("target"));
    }

    @Test(expected = HazelcastException.class)
    public void testCopy_failsWhenSourceCannotBeListed() {
        File source = mock(File.class);
        when(source.exists()).thenReturn(true);
        when(source.isDirectory()).thenReturn(true);
        when(source.listFiles()).thenReturn(null);
        when(source.getName()).thenReturn("dummy");

        File target = newFile("dest");
        assertFalse("Target folder should not exist yet", target.exists());
        assertTrue("Target folder should have been created", target.mkdir());

        copy(source, target);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCopy_failsWhenSourceIsDirAndTargetIsFile() throws Exception {
        File source = newFile("dir1");
        assertFalse("Source folder should not exist yet", source.exists());
        assertTrue("Source folder should have been created", source.mkdir());

        File target = newFile("file1");
        assertFalse("Target file should not exist yet", target.exists());
        assertTrue("Target file should have been created successfully", target.createNewFile());

        copy(source, target);
        fail("Expected a IllegalArgumentException thrown by copy()");
    }

    @Test
    public void testCopy_withInputStream() throws Exception {
        InputStream inputStream = null;
        try {
            File source = createFile("source");
            File target = createFile("target");

            writeTo(source, "test content");
            inputStream = new FileInputStream(source);

            copy(inputStream, target);

            assertTrue("source and target should have the same content", isEqualsContents(source, target));
        } finally {
            closeResource(inputStream);
        }
    }

    @Test(expected = HazelcastException.class)
    public void testCopy_withInputStream_failsWhenTargetNotExist() {
        InputStream source = mock(InputStream.class);
        File target = mock(File.class);
        when(target.exists()).thenReturn(false);

        copy(source, target);
    }

    @Test(expected = HazelcastException.class)
    public void testCopy_withInputStream_failsWhenSourceCannotBeRead() throws Exception {
        InputStream source = mock(InputStream.class);
        when(source.read(any(byte[].class))).thenThrow(new IOException("expected"));
        File target = createFile("target");

        copy(source, target);
    }

    @Test(expected = HazelcastException.class)
    public void testCopyFile_failsWhenTargetDoesntExistAndCannotBeCreated() throws Exception {
        File source = newFile("newFile");
        assertFalse("Source file should not exist yet", source.exists());
        assertTrue("Source file should have been created successfully", source.createNewFile());

        File target = mock(File.class);
        when(target.exists()).thenReturn(false);
        when(target.mkdirs()).thenReturn(false);

        copyFile(source, target, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCopyFile_failsWhenSourceDoesntExist() {
        File source = newFile("nonExistent");
        File target = newFile("target");

        copyFile(source, target, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCopyFile_failsWhenSourceIsNotAFile() {
        File source = newFile("source");
        assertFalse("Source folder should not exist yet", source.exists());
        assertTrue("Source folder should have been created", source.mkdir());

        File target = newFile("target");

        copyFile(source, target, -1);
    }

    @Test
    public void testDelete() {
        File file = createFile("file");

        delete(file);

        assertFalse("file should be deleted", file.exists());
    }

    @Test
    public void testDelete_shouldDoNothingWithNonExistentFile() {
        File file = newFile("notFound");

        delete(file);
    }

    @Test
    public void when_deleteSymlink_then_targetIntact() throws IOException {
        // Given
        File symLink = newFile("symLink");
        Path symLinkP = symLink.toPath();

        File target = createFile("dontDeleteMe");
        Path targetP = target.toPath();

        Files.createSymbolicLink(symLinkP, targetP);
        assertTrue(Files.exists(symLinkP, NOFOLLOW_LINKS));
        assertTrue(Files.exists(targetP, NOFOLLOW_LINKS));

        // When
        delete(symLink);

        // Then
        assertFalse(Files.exists(symLinkP, NOFOLLOW_LINKS));
        assertTrue(Files.exists(targetP, NOFOLLOW_LINKS));
    }

    @Test
    public void when_deleteBrokenSymlink_then_success() throws IOException {
        // Given
        File symLink = newFile("symLink");
        Path symLinkP = symLink.toPath();

        File target = newFile("doesntExist");
        Path targetP = target.toPath();

        Files.createSymbolicLink(symLinkP, targetP);
        assertTrue(Files.exists(symLinkP, NOFOLLOW_LINKS));

        // When
        delete(symLink);

        // Then
        assertFalse("File still exists after deletion", Files.exists(symLinkP, NOFOLLOW_LINKS));
    }

    @Test
    public void testDelete_shouldDeleteDirectoryRecursively() {
        File parentDir = createDirectory("parent");
        File file1 = createFile(parentDir, "file1");
        File file2 = createFile(parentDir, "file2");
        File childDir = createDirectory(parentDir, "child");
        File childFile1 = createFile(childDir, "childFile1");
        File childFile2 = createFile(childDir, "childFile2");

        delete(parentDir);

        assertFalse("parentDir should be deleted", parentDir.exists());
        assertFalse("file1 should be deleted", file1.exists());
        assertFalse("file2 should be deleted", file2.exists());
        assertFalse("childDir should be deleted", childDir.exists());
        assertFalse("childFile1 should be deleted", childFile1.exists());
        assertFalse("childFile2 should be deleted", childFile2.exists());
    }

    @Test
    public void testDeleteQuietly() {
        File file = createFile("file");

        deleteQuietly(file);

        assertFalse("file should be deleted", file.exists());
    }

    @Test
    public void testDeleteQuietly_shouldDoNothingWithNonExistentFile() {
        File file = newFile("notFound");

        deleteQuietly(file);
    }

    @Test
    public void testDeleteQuietly_shouldDeleteDirectoryRecursively() {
        File parentDir = createDirectory("parent");
        File file1 = createFile(parentDir, "file1");
        File file2 = createFile(parentDir, "file2");
        File childDir = createDirectory(parentDir, "child");
        File childFile1 = createFile(childDir, "childFile1");
        File childFile2 = createFile(childDir, "childFile2");

        deleteQuietly(parentDir);

        assertFalse("parentDir should be deleted", parentDir.exists());
        assertFalse("file1 should be deleted", file1.exists());
        assertFalse("file2 should be deleted", file2.exists());
        assertFalse("childDir should be deleted", childDir.exists());
        assertFalse("childFile1 should be deleted", childFile1.exists());
        assertFalse("childFile2 should be deleted", childFile2.exists());
    }

    @Test
    public void testDeleteQuietly_shouldDoNothingIfFileCouldNotBeDeleted() {
        File file = mock(File.class);
        when(file.exists()).thenReturn(true);
        when(file.delete()).thenReturn(false);

        deleteQuietly(file);
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

    @Test
    public void testCompactOrClearByteBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        buffer.put((byte) 0xFF);
        buffer.put((byte) 0xFF);
        upcast(buffer).flip();
        upcast(buffer).position(1);
        compactOrClear(buffer);
        assertEquals("Buffer position invalid", 1, buffer.position());

        buffer.put((byte) 0xFF);
        buffer.put((byte) 0xFF);
        compactOrClear(buffer);
        assertEquals("Buffer position invalid", 0, buffer.position());
    }

    @Test
    public void testCopyToHeapBuffer_whenSourceIsNull() {
        ByteBuffer dst = ByteBuffer.wrap(new byte[SIZE]);

        assertEquals(0, copyToHeapBuffer(null, dst));
    }

    @Test
    public void testCloseServerSocket_whenServerSocketThrows() throws Exception {
        ServerSocket serverSocket = mock(ServerSocket.class);
        doThrow(new IOException()).when(serverSocket).close();
        try {
            close(serverSocket);
        } catch (Exception ex) {
            fail("IOUtils should silently close server socket when exception thrown");
        }
    }

    @Test(expected = HazelcastException.class)
    public void testRename_whenFileNowNotExist() {
        File toBe = mock(File.class);

        File now = mock(File.class);
        when(now.renameTo(toBe)).thenReturn(false);
        when(now.exists()).thenReturn(false);

        rename(now, toBe);
    }

    @Test(expected = HazelcastException.class)
    public void testRename_whenFileToBeNotExist() {
        File toBe = mock(File.class);
        when(toBe.exists()).thenReturn(false);

        File now = mock(File.class);
        when(now.renameTo(toBe)).thenReturn(false);
        when(now.exists()).thenReturn(true);

        rename(now, toBe);
    }

    @Test(expected = HazelcastException.class)
    public void testRename_whenFileToBeNotDeleted() {
        File toBe = mock(File.class);
        when(toBe.exists()).thenReturn(true);
        when(toBe.delete()).thenReturn(false);

        File now = mock(File.class);
        when(now.renameTo(toBe)).thenReturn(false);
        when(now.exists()).thenReturn(true);

        rename(now, toBe);
    }

    @Test
    public void testGetPath_shouldFormat() {
        String root = "root";
        String parent = "parent";
        String child = "child";
        String expected = format("%s%s%s%s%s", root, File.separator, parent, File.separator, child);
        String actual = getPath(root, parent, child);
        assertEquals(expected, actual);
    }

    @Test
    public void testMove_targetDoesntExist() throws IOException {
        Path src = tempFolder.newFile("source.txt").toPath();
        Path target = src.resolveSibling("target.txt");

        assertMoveInternal(src, target, false);
        assertMoveInternal(target, src, true);
    }

    @Test
    public void testMove_targetExist() throws IOException {
        Path src = tempFolder.newFile("source.txt").toPath();
        Path target = src.resolveSibling("target.txt");
        Files.write(target, "foo".getBytes(UTF_8), CREATE);
        assertMoveInternal(src, target, false);
        Files.write(src, "bar".getBytes(UTF_8), CREATE);
        assertMoveInternal(target, src, true);
    }

    @Test
    public void testMove_sourceDoesntExist() throws IOException {
        Path src = tempFolder.newFile("source.txt").toPath();
        Files.delete(src);
        Path target = src.resolveSibling("target.txt");
        Assert.assertThrows(NoSuchFileException.class, () -> IOUtil.move(src, target));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPath_whenPathsInvalid() {
        getPath();
    }

    private File newFile(String filename) {
        File file = new File(uniquize(filename));
        files.add(file);
        return file;
    }

    private File newFile(File parent, String filename) {
        File file = new File(parent, uniquize(filename));
        files.add(file);
        return file;
    }

    private String uniquize(String filename) {
        String name = IOUtilTest.class.getSimpleName() + '-' + testName.getMethodName() + '-' + filename;
        return name.substring(0, min(255, name.length()));
    }

    private File createFile(String fileName) {
        return createFile(newFile(fileName));
    }

    private File createFile(File parent, String fileName) {
        return createFile(newFile(parent, fileName));
    }

    private File createFile(File file) {
        files.add(file);
        if (file.isFile()) {
            return file;
        }
        try {
            if (!file.createNewFile() || !file.exists()) {
                fail("Could not create file " + file.getAbsolutePath());
            }
        } catch (IOException e) {
            fail("Could not create file " + file.getAbsolutePath() + ": " + e.getMessage());
        }
        return file;
    }

    @SuppressWarnings("SameParameterValue")
    private File createDirectory(String dirName) {
        return createDirectory(newFile(dirName));
    }

    @SuppressWarnings("SameParameterValue")
    private File createDirectory(File parent, String dirName) {
        return createDirectory(newFile(parent, dirName));
    }

    private File createDirectory(File dir) {
        files.add(dir);
        if (dir.isDirectory()) {
            return dir;
        }
        if (!dir.mkdirs() || !dir.exists()) {
            fail("Could not create directory " + dir.getAbsolutePath());
        }
        return dir;
    }

    private static void writeTo(File file, String content) {
        FileWriter writer = null;
        try {
            writer = new FileWriter(file);
            writer.write(content);
        } catch (IOException e) {
            throw rethrow(e);
        } finally {
            closeResource(writer);
        }
    }

    private static void assertEqualFiles(File f1, File f2) {
        assertEquals("f1 and f2 should have the same name", f1.getName(), f2.getName());
        assertEquals("f1 and f2 should both exist or not exist", f1.exists(), f2.exists());
        if (f1.isFile()) {
            assertTrue("f1 is a file, but f2 is not", f2.isFile());
            assertTrue("f1 and f2 should have the same content", isEqualsContents(f1, f2));
            return;
        }
        assertEquals(f1.isDirectory(), f2.isDirectory());
        File[] f1Files = f1.listFiles();
        File[] f2Files = f2.listFiles();
        assertNotNull("f1.listFiles() should not return null ", f1Files);
        assertNotNull("f2.listFiles() should not return null ", f2Files);
        assertEquals(f1Files.length, f2Files.length);
        for (File f1File : f1Files) {
            assertEqualFiles(f1File, new File(f2, f1File.getName()));
        }
        for (File f2File : f2Files) {
            assertEqualFiles(f2File, new File(f1, f2File.getName()));
        }
    }

    // Note: use only for small files (e.g. up to a couple of hundred KBs). See below.
    private static boolean isEqualsContents(File f1, File f2) {
        InputStream is1 = null;
        InputStream is2 = null;
        try {
            is1 = new FileInputStream(f1);
            is2 = new FileInputStream(f2);
            // compare byte-by-byte since InputStream.read() possibly doesn't return the requested number of bytes
            // this is why this method should be used for smallFiles
            int data;
            while ((data = is1.read()) != -1) {
                if (data != is2.read()) {
                    return false;
                }
            }
            return is2.read() == -1;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            closeResource(is1);
            closeResource(is2);
        }
    }

    private void assertMoveInternal(Path src, Path target, boolean withTimeout) throws IOException {
        Files.write(src, "Hazelcast".getBytes(UTF_8), TRUNCATE_EXISTING);
        if (withTimeout) {
            IOUtil.moveWithTimeout(src, target, Duration.ofSeconds(2));
        } else {
            IOUtil.move(src, target);
        }

        assertFalse(Files.exists(src));
        assertTrue(Files.exists(target));
        List<String> lines = Files.readAllLines(target);
        assertEquals(1, lines.size());
        assertEquals("Hazelcast", lines.get(0));
    }
}
