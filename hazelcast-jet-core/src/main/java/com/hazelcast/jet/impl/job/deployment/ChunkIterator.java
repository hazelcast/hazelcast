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

package com.hazelcast.jet.impl.job.deployment;

import com.hazelcast.jet.config.DeploymentConfig;
import com.hazelcast.jet.impl.util.JetUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public final class ChunkIterator implements Iterator<Chunk> {
    private final int chunkSize;
    private final Iterator<DeploymentConfig> configIterator;
    private InputStream inputStream;
    private DeploymentConfig deploymentConfig;
    private AtomicInteger sequenceGenerator = new AtomicInteger();

    public ChunkIterator(Set<DeploymentConfig> deploymentConfigs, int chunkSize) {
        this.configIterator = deploymentConfigs.iterator();
        this.chunkSize = chunkSize;
    }

    private void switchFile() throws IOException {
        if (!hasMoreDeployments()) {
            throw new NoSuchElementException();
        }
        deploymentConfig = configIterator.next();
        inputStream = deploymentConfig.getUrl().openStream();
        sequenceGenerator.set(0);
    }

    @Override
    public boolean hasNext() {
        try {
            return hasMoreDeployments() || (streamIsNotNull() && streamHasAvailableBytes());
        } catch (IOException e) {
            throw unchecked(e);
        }
    }

    private boolean hasMoreDeployments() {
        return configIterator.hasNext();
    }

    private boolean streamIsNotNull() {
        return inputStream != null;
    }

    private boolean streamHasAvailableBytes() throws IOException {
        return inputStream.available() > 0;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Chunk next() {
        try {
            if (inputStream == null) {
                switchFile();
            }

            byte[] bytes = JetUtil.readChunk(inputStream, chunkSize);

            if (inputStream.available() <= 0) {
                close(inputStream);
                inputStream = null;
            }

            if (bytes.length > 0) {
                return new Chunk(
                        bytes,
                        deploymentConfig.getDescriptor(),
                        chunkSize,
                        bytes.length,
                        sequenceGenerator.incrementAndGet()
                );
            } else {
                throw new NoSuchElementException();
            }
        } catch (IOException e) {
            throw unchecked(e);
        }
    }

    private void close(InputStream inputStream) throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }
}
