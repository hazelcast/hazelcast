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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hazelcast.jet.impl.util.Util.read;
import static com.hazelcast.util.ExceptionUtil.rethrow;

public final class ResourceIterator implements Iterator<ResourcePart> {
    private final int partSize;
    private final Iterator<ResourceConfig> configIterator;
    private InputStream inputStream;
    private ResourceConfig resourceConfig;
    private int offset;

    public ResourceIterator(Set<ResourceConfig> resourceConfigs, int partSize) {
        this.configIterator = resourceConfigs.iterator();
        this.partSize = partSize;
    }

    private void switchFile() throws IOException {
        if (!configIterator.hasNext()) {
            throw new NoSuchElementException();
        }
        resourceConfig = configIterator.next();
        inputStream = resourceConfig.getUrl().openStream();
        offset = 0;
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
    public ResourcePart next() {
        try {
            if (inputStream == null) {
                switchFile();
            }

            byte[] bytes = read(inputStream, partSize);

            if (inputStream.available() <= 0) {
                close(inputStream);
                inputStream = null;
            }
            if (bytes.length > 0) {
                ResourcePart part = new ResourcePart(resourceConfig.getDescriptor(), bytes, offset);
                offset += bytes.length;
                return part;
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

}
