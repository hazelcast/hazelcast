/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.jet.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.security.permission.ConnectorPermission;
import com.hazelcast.vector.VectorValues;

import javax.annotation.Nonnull;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serial;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permission;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singletonList;

public final class VectorFiles {
    private VectorFiles() {
    }

    public static FunctionEx<? super Path, ? extends Stream<Map.Entry<Integer, VectorValues>>> fvecsFileFn(
            String directory
    ) {
        return new FunctionEx<>() {
            @Serial
            private static final long serialVersionUID = 1L;

            @Override
            public Stream<Map.Entry<Integer, VectorValues>> applyEx(Path path) throws Exception {
                return mapSequenceFrom(path, VectorFiles::getFloats);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(ConnectorPermission.file(directory, ACTION_READ));
            }
        };
    }

    public static FunctionEx<? super Path, ? extends Stream<Map.Entry<Integer, int[]>>> ivecsFileFn(
            String directory
    ) {
        return new FunctionEx<>() {
            @Serial
            private static final long serialVersionUID = 1L;

            @Override
            public Stream<Map.Entry<Integer, int[]>> applyEx(Path path) throws Exception {
                return mapSequenceFrom(path, VectorFiles::getInts);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(ConnectorPermission.file(directory, ACTION_READ));
            }
        };
    }

    @Nonnull
    public static <V> Stream<Map.Entry<Integer, V>> mapSequenceFrom(Path path, FunctionEx<DataInputStream, V> itemReader)
            throws IOException {
        var dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        var spliterator = Spliterators.spliteratorUnknownSize(mapSequenceFrom(dis, itemReader),
                Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false);
    }

    public static <V> Iterator<Map.Entry<Integer, V>> mapSequenceFrom(DataInputStream dis,
                                                                      FunctionEx<DataInputStream, V> itemReader) {
        return new Iterator<>() {
            private boolean closed;
            private int index;
            @Override
            public boolean hasNext() {
                try {
                    if (!closed && dis.available() > 0) {
                        return true;
                    }
                    dis.close();
                    closed = true;
                    return false;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Map.Entry<Integer, V> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return Map.entry(index++, itemReader.apply(dis));
            }
        };
    }

    public static VectorValues getFloats(DataInputStream dis) throws IOException {
        int dimension = Integer.reverseBytes(dis.readInt());
        assert dimension > 0 : dimension;
        var buffer = new byte[dimension * Float.BYTES];
        dis.readFully(buffer);
        var byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);

        var vector = new float[dimension];
        var floatBuffer = byteBuffer.asFloatBuffer();
        floatBuffer.get(vector);
        return VectorValues.of(vector);
    }

    public static int[] getInts(DataInputStream dis) throws IOException {
        var numNeighbors = Integer.reverseBytes(dis.readInt());
        var neighbors = new int[numNeighbors];

        for (var i = 0; i < numNeighbors; i++) {
            var neighbor = Integer.reverseBytes(dis.readInt());
            neighbors[i] = neighbor;
        }
        return neighbors;
    }
}
