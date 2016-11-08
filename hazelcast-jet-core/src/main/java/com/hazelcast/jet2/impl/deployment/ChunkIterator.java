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

package com.hazelcast.jet2.impl.deployment;

import com.hazelcast.jet2.DeploymentConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public final class ChunkIterator implements Iterator<ResourceChunk> {
    private final int chunkSize;
    private final Iterator<DeploymentConfig> configIterator;
    private InputStream inputStream;
    private DeploymentConfig deploymentConfig;
    private int sequence;

    public ChunkIterator(Set<DeploymentConfig> deploymentConfigs, int chunkSize) {
        this.configIterator = deploymentConfigs.iterator();
        this.chunkSize = chunkSize;
    }

    private void switchFile() throws IOException {
        if (!configIterator.hasNext()) {
            throw new NoSuchElementException();
        }
        deploymentConfig = configIterator.next();
        inputStream = deploymentConfig.getUrl().openStream();
        sequence = 0;
    }

    @Override
    public boolean hasNext() {
        try {
            return configIterator.hasNext() || (inputStream != null && inputStream.available() > 0);
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResourceChunk next() {
        try {
            if (inputStream == null) {
                switchFile();
            }

            byte[] bytes = readChunk(inputStream, chunkSize);

            if (inputStream.available() <= 0) {
                close(inputStream);
                inputStream = null;
            }

            if (bytes.length > 0) {
                return new ResourceChunk(bytes, deploymentConfig.getDescriptor(), sequence++);
            } else {
                throw new NoSuchElementException();
            }
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private static void close(InputStream inputStream) throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    private static byte[] readChunk(InputStream in, int chunkSize) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] b = new byte[chunkSize];
            out.write(b, 0, in.read(b));
            return out.toByteArray();
        }
    }

}
