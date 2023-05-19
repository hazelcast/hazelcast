/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;

public final class FileTestSupport {

    private FileTestSupport() {
    }

    public static File randomTmpFile() {
        return randomTmpFile(0);
    }

    public static File randomTmpFile(long size) {
        String tmpdir = System.getProperty("java.io.tmpdir");
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String separator = FileSystems.getDefault().getSeparator();
        String path = tmpdir + separator + uuid;
        File file = new File(path);
        file.deleteOnExit();

        if (size > 0) {
            try {
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    for (long k = 0; k < size; k++) {
                        fos.write(1);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return file;
    }

    public static void write(File file, String content) {
        try {
            FileWriter myWriter = new FileWriter(file);
            myWriter.write(content);
            myWriter.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void assertSameContent(File expected, File actual) {
        byte[] expectedBytes = load(expected);
        byte[] actualBytes = load(actual);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    public static byte[] load(File file) {
        try {
            try (FileInputStream fis = new FileInputStream(file)) {
                return fis.readAllBytes();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
