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

package com.hazelcast.jet.internal.impl.util;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.jet.internal.api.hazelcast.JetService;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.config.JetClientConfig;
import com.hazelcast.jet.api.config.JetConfig;
import com.hazelcast.spi.NodeEngine;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Utility class for base JET-functionality
 */
public final class JetUtil {
    /**
     * Represents maximal value for the length of JET-application
     */
    public static final int MAX_APPLICATION_NAME_SIZE = 32;

    /**
     * Number of bytes in kilobyte
     */
    public static final int KILOBYTE = 1024;

    public static final int MEGABYTE = KILOBYTE * KILOBYTE;

    public static final int GIGABYTE = MEGABYTE * KILOBYTE;

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
            InputStreamReader raf = new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8"));
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

    private static boolean isEmptyConfig(final JetApplicationConfig jetApplicationConfig) {
        return (jetApplicationConfig == null) || (jetApplicationConfig.getName() == null);
    }

    public static JetApplicationConfig resolveJetDefaultApplicationConfig(final NodeEngine nodeEngine) {
        JetApplicationConfig jetApplicationConfig = null;

        if (isJetConfig(nodeEngine.getConfig())) {
            jetApplicationConfig =
                    ((JetConfig) nodeEngine.getConfig()).getJetApplicationConfig(JetService.SERVICE_NAME);
        }

        if (isEmptyConfig(jetApplicationConfig)) {
            jetApplicationConfig = new JetApplicationConfig();
        }

        return jetApplicationConfig;
    }

    public static JetApplicationConfig resolveJetServerApplicationConfig(final NodeEngine nodeEngine,
                                                                         final JetApplicationConfig jetApplicationConfig,
                                                                         final String name) {
        return resolveJetApplicationConfig(nodeEngine.getConfig(), jetApplicationConfig, name);
    }

    public static JetApplicationConfig resolveJetClientApplicationConfig(final HazelcastClientInstanceImpl client,
                                                                         final JetApplicationConfig jetApplicationConfig,
                                                                         final String name) {
        return resolveJetApplicationConfig(client.getClientConfig(), jetApplicationConfig, name);
    }

    public static List<Integer> getLocalPartitions(final NodeEngine nodeEngine) {
        return nodeEngine.getPartitionService().getMemberPartitions(nodeEngine.getThisAddress());
    }

    public static boolean isPartitionLocal(final NodeEngine nodeEngine, int partitionId) {
        return nodeEngine.getPartitionService().getPartition(partitionId).isLocal();
    }

    private static <T> JetApplicationConfig resolveJetApplicationConfig(final T hazelcastConfig,
                                                                        final JetApplicationConfig jetApplicationConfig,
                                                                        final String name) {
        JetApplicationConfig resultConfig = jetApplicationConfig;

        if (isEmptyConfig(resultConfig)) {
            if (isJetConfig(hazelcastConfig)) {
                resultConfig = ((JetConfig) hazelcastConfig).getJetApplicationConfig(name);
            } else if (isJetClientConfig(hazelcastConfig)) {
                resultConfig = ((JetClientConfig) hazelcastConfig).getJetApplicationConfig(name);
            }
        }

        if (isEmptyConfig(resultConfig)) {
            resultConfig = new JetApplicationConfig(name);
        }

        return resultConfig;
    }

    private static boolean isJetClientConfig(final Object hazelcastConfig) {
        return ((hazelcastConfig != null) && (hazelcastConfig instanceof JetClientConfig));
    }

    private static boolean isJetConfig(final Object hazelcastConfig) {
        return ((hazelcastConfig != null) && (hazelcastConfig instanceof JetConfig));
    }
}
