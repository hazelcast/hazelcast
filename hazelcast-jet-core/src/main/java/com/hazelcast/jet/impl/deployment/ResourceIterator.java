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

import com.hazelcast.jet.config.ResourceConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public final class ResourceIterator implements Iterator<ResourcePart>, AutoCloseable {

    static final int RESOURCE_PART_SIZE = 1 << 14;

    private final Iterator<ResourceConfig> configIterator;
    private InputStream inputStream;
    private ResourceConfig resourceConfig;
    private int offset;
    private byte[] buffer;
    private ResourcePart nextPart;

    public ResourceIterator(Set<ResourceConfig> resourceConfigs) {
        this.configIterator = resourceConfigs.iterator();
        buffer = new byte[RESOURCE_PART_SIZE];
        readNext();
    }

    private void switchFile() throws IOException {
        resourceConfig = configIterator.next();
        inputStream = resourceConfig.getUrl().openStream();
        offset = 0;
    }

    @Override
    public boolean hasNext() {
        return nextPart != null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResourcePart next() {
        if (nextPart  == null) {
            throw new NoSuchElementException();
        }
        try {
            return nextPart;
        } finally {
            readNext();
        }
    }

    private void readNext() {
        try {
            nextPart = null;
            while (nextPart == null && (configIterator.hasNext() || inputStream != null)) {
                if (inputStream == null) {
                    switchFile();
                }

                int bytesRead = inputStream.read(buffer);

                if (bytesRead < 0) {
                    bytesRead = 0;
                    inputStream.close();
                    inputStream = null;
                    // only output zero-length parts for empty files
                    if (offset > 0) {
                        continue;
                    }
                }

                nextPart = new ResourcePart(resourceConfig.getDescriptor(), Arrays.copyOf(buffer, bytesRead), offset);
                offset += bytesRead;
            }
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    @Override
    public void close() {
        if (inputStream != null) {
            uncheckRun(() -> inputStream.close());
        }
    }
}
