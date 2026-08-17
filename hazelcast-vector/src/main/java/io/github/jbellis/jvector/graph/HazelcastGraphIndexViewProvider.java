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

package io.github.jbellis.jvector.graph;

import io.github.jbellis.jvector.util.Bits;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * This class is used only to provide a graph index view for constructing a GraphSearcher.
 * As of version 3, the public constructor GraphSearcher(GraphIndex.View view) is no longer available.
 * <p>
 * Declared in the {@code jvector} package because some {@link OnHeapGraphIndex} methods (specifically
 * {@link OnHeapGraphIndex#entry()} / {@link OnHeapGraphIndex#getNeighbors(int)}) are package-protected.
 */
public class HazelcastGraphIndexViewProvider implements GraphIndex {

    private final HazelcastGraphIndexView indexView;


    public HazelcastGraphIndexViewProvider(OnHeapGraphIndex graphIndex) {
        this.indexView = new HazelcastGraphIndexView(graphIndex);
    }

    @Override
    public View getView() {
        return indexView;
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("View holder does not support this operation.");
    }

    @Override
    public NodesIterator getNodes() {
        throw new UnsupportedOperationException("View holder does not support this operation.");
    }


    @Override
    public int maxDegree() {
        throw new UnsupportedOperationException("View holder does not support this operation.");
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("View holder does not support this operation.");
    }

    @Override
    public long ramBytesUsed() {
        throw new UnsupportedOperationException("View holder does not support this operation.");
    }

    public static class HazelcastGraphIndexView implements GraphIndex.View {

        private static final NodesIterator EMPTY_ITERATOR = new NodesIterator(0) {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public int nextInt() {
                throw new NoSuchElementException();
            }
        };

        private final OnHeapGraphIndex index;

        private HazelcastGraphIndexView(OnHeapGraphIndex index) {
            this.index = index;
        }

        @Override
        public NodesIterator getNeighborsIterator(int node) {
            var neighbors = index.getNeighbors(node);
            if (neighbors == null) {
                return EMPTY_ITERATOR;
            }

            return neighbors.iterator();
        }

        @Override
        public int size() {
            return index.size();
        }

        @Override
        public int entryNode() {
            return index.entry();
        }

        @Override
        public String toString() {
            return "HazelcastGraphIndexView(size=" + size() + ", entryPoint=" + index.entry();
        }

        @Override
        public Bits liveNodes() {
            return index.getDeletedNodes().cardinality() == 0 ? Bits.ALL : Bits.inverseOf(index.getDeletedNodes());
        }

        @Override
        public int getIdUpperBound() {
            return index.getIdUpperBound();
        }

        @Override
        public void close() {
        }
    }
}
