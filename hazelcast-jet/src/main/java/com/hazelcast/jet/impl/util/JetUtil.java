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

package com.hazelcast.jet.impl.util;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

public final class JetUtil {
    public static final int MAX_APPLICATION_NAME_SIZE = 32;
    public static final int KILOBYTE = 1024;

    private JetUtil() {
    }

    public static void close(Closeable... closeables) {
        if (closeables == null) {
            return;
        }

        Throwable lastError = null;

        for (Closeable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    lastError = e;
                }
            }
        }

        if (lastError != null) {
            throw JetUtil.reThrow(lastError);
        }
    }

    public static byte[] readChunk(InputStream in, int chunkSize) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            byte[] b = new byte[chunkSize];
            out.write(b, 0, in.read(b));
            return out.toByteArray();
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        } finally {
            close(out);
        }
    }

    public static byte[] readFully(InputStream in) {
        return readFully(in, false);
    }

    public static byte[] readFully(InputStream in, boolean closeInput) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            int len;
            byte[] b = new byte[KILOBYTE];

            while ((len = in.read(b)) > 0) {
                out.write(b, 0, len);
            }

            return out.toByteArray();
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        } finally {
            if (closeInput) {
                close(in, out);
            } else {
                close(out);
            }
        }
    }

    public static long[] splitFile(File file, int chunkCount) {
        try {
            FileReader raf = new FileReader(file);
            long[] offsets = new long[chunkCount];
            Arrays.fill(offsets, -1L);

            try {
                long offset = 0;
                offsets[0] = 0;

                for (int i = 1; i < chunkCount; i++) {
                    long delta = (file.length() / chunkCount);
                    long newOffset = offset + delta;

                    if (newOffset >= file.length()) {
                        break;
                    }

                    raf.skip(delta);
                    offset = newOffset;

                    while (true) {
                        int read = raf.read();
                        offset++;
                        if (read == '\n' || read == -1) {
                            break;
                        }
                    }

                    offsets[i] = offset;
                }
            } finally {
                raf.close();
            }

            return offsets;
        } catch (Throwable e) {
            throw JetUtil.reThrow(e);
        }
    }

    public static RuntimeException reThrow(Throwable e) {
        if (e instanceof ExecutionException) {
            if (e.getCause() != null) {
                throw reThrow(e.getCause());
            } else {
                throw new RuntimeException(e);
            }
        }

        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }

        return new RuntimeException(e);
    }

    public static boolean isEmpty(Collection collection) {
        return (collection == null || collection.isEmpty());
    }

    public static boolean isEmpty(Object[] object) {
        return (object == null || object.length == 0);
    }

    public static void checkApplicationName(String applicationName) {
        checkNotNull(
                applicationName,
                "Retrieving an application instance with a null name is not allowed!"
        );

        checkTrue(
                applicationName.length() <= MAX_APPLICATION_NAME_SIZE,
                "Length of applicationName shouldn't be greater than " + MAX_APPLICATION_NAME_SIZE
        );
    }
}
