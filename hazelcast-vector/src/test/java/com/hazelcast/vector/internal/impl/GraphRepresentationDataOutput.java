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

package com.hazelcast.vector.internal.impl;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides the ability to display a jVector graph in a human-readable format.
 * It is intended for testing purposes.
 */

public class GraphRepresentationDataOutput implements DataOutput {

    private static final int EMPTY = -1;

    private final List<Integer> header = new ArrayList<>();
    private final Map<Integer, List<Integer>> graph = new HashMap<>();
    private int mapNode = EMPTY;
    private int sizeNode = EMPTY;
    private int counter;

    public Map<Integer, List<Integer>> getGraph() {
        return graph;
    }

    public String hash() {
        return String.valueOf(graph.hashCode());
    }

    public int entry() {
        return header.get(1);
    }

    @Override
    public void write(int b) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void write(byte[] b) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void write(byte[] b, int off, int len) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeBoolean(boolean v) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeByte(int v) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeShort(int v) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeChar(int v) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeInt(int v) throws IOException {
        if (header.size() < 3) {
            header.add(v);
            return;
        }
        if (mapNode == EMPTY) {
            graph.put(v, new ArrayList<>());
            mapNode = v;
            return;
        }
        if (sizeNode == EMPTY) {
            sizeNode = v;
            return;
        }
        if (counter < sizeNode) {
            graph.get(mapNode).add(v);
            counter++;
            if (counter == sizeNode) {
                mapNode = EMPTY;
                sizeNode = EMPTY;
                counter = 0;
            }
        }


    }

    @Override
    public void writeLong(long v) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeFloat(float v) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeDouble(double v) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeBytes(String s) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeChars(String s) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeUTF(String s) {
        throw new UnsupportedOperationException("Not implemented");
    }
}

