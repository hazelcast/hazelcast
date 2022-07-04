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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.ByteBuffer;

import static com.hazelcast.jet.impl.util.IOUtil.copyStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class IMapInputOutputStreamTest extends SimpleTestInClusterSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void test_writeToClosedStream_then_throwsException() throws IOException {
        IMap<String, byte[]> map = instance().getMap(randomMapName());
        IMapOutputStream outputStream = new IMapOutputStream(map, "test");

        outputStream.close();
        expectedException.expect(IOException.class);
        outputStream.write(5);
    }

    @Test
    public void test_multipleCallsToCloseStream_then_flushesOnlyOnce() throws IOException {
        IMap<String, byte[]> map = instance().getMap(randomMapName());
        IMapOutputStream outputStream = new IMapOutputStream(map, "test");

        outputStream.close();
        long putOperationCountBeforeCloses = map.getLocalMapStats().getPutOperationCount();
        outputStream.close();
        outputStream.close();
        outputStream.close();

        assertEquals(putOperationCountBeforeCloses, map.getLocalMapStats().getPutOperationCount());
    }

    @Test
    public void test_readFromClosedStream_then_throwsException() throws IOException {
        IMap<String, byte[]> map = instance().getMap(randomMapName());
        map.put("test", new byte[] {0, 0, 0, 4});
        IMapInputStream inputStream = new IMapInputStream(map, "test");

        inputStream.close();
        expectedException.expect(IOException.class);
        System.out.println(inputStream.read(new byte[] {1}, 0, 1));
    }

    @Test
    public void test_writeOutOfBounds_then_throwsException() throws IOException {
        IMap<String, byte[]> map = instance().getMap(randomMapName());
        IMapOutputStream outputStream = new IMapOutputStream(map, "test");

        expectedException.expect(IndexOutOfBoundsException.class);
        outputStream.write(new byte[] {1}, 5, 5);
    }

    @Test
    public void test_readOutOfBounds_then_throwsException() throws IOException {
        IMap<String, byte[]> map = instance().getMap(randomMapName());
        map.put("test", new byte[] {0, 0, 0, 4});
        IMapInputStream inputStream = new IMapInputStream(map, "test");

        expectedException.expect(IndexOutOfBoundsException.class);
        System.out.println(inputStream.read(new byte[] {1}, 5, 5));
    }

    @Test
    public void test_writeFile_to_IMap_then_fileReadFromIMapAsByteArray() throws Exception {
        // Given
        URL resource = this.getClass().getClassLoader().getResource("deployment/resource.txt");
        assertNotNull(resource);
        File file = new File(resource.toURI());
        long length = file.length();
        IMap<String, byte[]> map = instance().getMap(randomMapName());

        // When
        try (
                InputStream in = resource.openStream();
                IMapOutputStream ios = new IMapOutputStream(map, "test")
        ) {
            copyStream(in, ios);
        }

        // Then
        byte[] bytes = map.get("test");
        Assert.assertNotNull(bytes);
        int numChunks = ByteBuffer.wrap(bytes).getInt();
        assertEquals(numChunks + 1, map.size());
        int lengthFromMap = 0;
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) length);
        for (int i = 0; i < numChunks; i++) {
            byte[] chunk = map.get("test_" + i);
            byteBuffer.put(chunk);
            lengthFromMap += chunk.length;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(byteBuffer.array())));
        assertTrue(reader.readLine().startsWith("AAAP|Advanced"));
        assertEquals(length, lengthFromMap);
    }

    @Test
    public void test_writeFile_to_IMap_then_fileReadFromIMap_with_IMapInputStream() throws Exception {
        // Given
        URL resource = this.getClass().getClassLoader().getResource("deployment/resource.txt");
        assertNotNull(resource);
        File file = new File(resource.toURI());
        long length = file.length();
        IMap<String, byte[]> map = instance().getMap(randomMapName());

        // When
        try (
                InputStream inputStream = resource.openStream();
                IMapOutputStream ios = new IMapOutputStream(map, "test")
        ) {
            copyStream(inputStream, ios);
        }

        // Then
        try (IMapInputStream iMapInputStream = new IMapInputStream(map, "test")) {
            byte[] bytes = new byte[(int) length];
            BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes)));
            assertTrue(IOUtil.readFullyOrNothing(iMapInputStream, bytes));
            assertTrue(reader.readLine().startsWith("AAAP|Advanced"));
        }
    }
}
