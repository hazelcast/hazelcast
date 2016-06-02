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

package com.hazelcast.jet.internal.impl.application.localization;

import com.hazelcast.jet.internal.impl.application.LocalizationResource;
import com.hazelcast.jet.internal.impl.util.JetUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public final class ChunkIterator implements Iterator<Chunk> {
    private final int chunkSize;
    private final Iterator<LocalizationResource> urlIterator;
    private long currentLength;
    private InputStream currentInputStream;
    private LocalizationResource currentURL;

    public ChunkIterator(Set<LocalizationResource> urls, int chunkSize) {
        this.urlIterator = urls.iterator();
        this.chunkSize = chunkSize;
    }

    private void switchFile() throws IOException {
        if (!this.urlIterator.hasNext()) {
            throw new NoSuchElementException();
        }

        this.currentURL = this.urlIterator.next();
        this.currentInputStream = this.currentURL.openStream();
        this.currentLength = this.currentInputStream.available();
    }

    @Override
    public boolean hasNext() {
        try {
            return (this.urlIterator.hasNext()) || (
                    (this.currentInputStream != null)
                            &&
                            (this.currentInputStream.available() > 0)

            );
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public void remove() {
        throw new IllegalStateException();
    }

    @Override
    public Chunk next() {
        try {
            if (this.currentInputStream == null) {
                do {
                    switchFile();
                } while (this.currentInputStream.available() <= 0);
            }

            if (this.currentInputStream.available() <= 0) {
                throw new NoSuchElementException();
            }

            byte[] bytes = JetUtil.readChunk(this.currentInputStream, this.chunkSize);

            if (this.currentInputStream.available() <= 0) {
                close(this.currentInputStream);
                this.currentInputStream = null;
            }

            if (bytes.length > 0) {
                return new Chunk(
                        bytes,
                        this.currentURL.getDescriptor(),
                        this.currentLength
                );
            } else {
                throw new NoSuchElementException();
            }
        } catch (FileNotFoundException e) {
            throw JetUtil.reThrow(e);
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    private void close(InputStream inputStream) throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }
}
