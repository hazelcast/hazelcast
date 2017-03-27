/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Distributed.IntFunction;
import com.hazelcast.jet.config.ResourceConfig;
import org.junit.Test;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ResourceIteratorTest {

    @Test
    public void test_nonEmptyFiles() throws IOException {
        doTest(3, i -> "contents" + i);
    }

    @Test
    public void test_allEmptyFiles() throws IOException {
        doTest(3, i -> "");
    }

    @Test
    public void test_firstEmptyFile() throws IOException {
        doTest(3, i -> i == 0 ? "" : "contents" + i);
    }

    @Test
    public void test_middleEmptyFile() throws IOException {
        doTest(3, i -> i == 1 ? "" : "contents" + i);
    }

    @Test
    public void test_lastEmptyFile() throws IOException {
        doTest(3, i -> i == 2 ? "" : "contents" + i);
    }

    private void doTest(int numFiles, IntFunction<String> contentsFactory) throws IOException {
        Path directory = null;
        Path[] files = null;

        try {
            // create files and their contents
            directory = Files.createTempDirectory(ResourceIteratorTest.class.getSimpleName());

            files = new Path[numFiles];
            ResourceConfig[] configs = new ResourceConfig[numFiles];
            String[] contents = new String[numFiles];

            for (int i = 0; i < numFiles; i++) {
                files[i] = directory.resolve("file" + i);
                contents[i] = contentsFactory.apply(i);
                try (Writer writer = Files.newBufferedWriter(files[i])) {
                    writer.append(contents[i]);
                }
                configs[i] = new ResourceConfig(files[i].toUri().toURL(), String.valueOf(i), null);
            }

            // create the ResourceIterator
            int partSize = 5;
            try (ResourceIterator ri = new ResourceIterator(new LinkedHashSet<>(Arrays.asList(configs)), partSize)) {
                // iterate it and check, that the contents match
                int lastIndex = -1;
                int lastOffset = 0;
                while (ri.hasNext()) {
                    ResourcePart rp = ri.next();
                    int fileIndex = Integer.parseInt(rp.getDescriptor().getId());
                    assertTrue("part is too big", rp.getBytes().length <= partSize);
                    if (contents[fileIndex].length() > 0) {
                        assertTrue("zero-length part for non-empty file", rp.getBytes().length > 0);
                    }
                    assertTrue("some config was skipped", lastIndex == fileIndex || lastIndex + 1 == fileIndex);

                    if (fileIndex > lastIndex) {
                        if (lastIndex >= 0) {
                            assertEquals("config not fully read", contents[lastIndex].length(), lastOffset);
                        }
                        lastIndex = fileIndex;
                        lastOffset = 0;
                    }
                    assertEquals("offsets not in sequence", lastOffset, rp.getOffset());
                    lastOffset += rp.getBytes().length;
                    assertEquals("contents don't match", contents[fileIndex].substring(rp.getOffset(), lastOffset), new String(rp.getBytes()));
                }
                assertEquals("not all config files read", lastIndex, numFiles - 1);
                assertEquals("config not fully read", contents[lastIndex].length(), lastOffset);
            }
        } finally {
            if (files != null) {
                for (Path file : files) {
                    Files.delete(file);
                }
            }
            if (directory != null) {
                Files.delete(directory);
            }
        }
    }

}
