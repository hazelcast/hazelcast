/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Consumer;

class StreamToStreamJoinHeapBuffer extends StreamToStreamJoinBuffer {
    private final PriorityQueue<JetSqlRow> buffer;
    private final ToLongFunctionEx<JetSqlRow> timeExtractor;

    StreamToStreamJoinHeapBuffer(List<Map.Entry<Byte, ToLongFunctionEx<JetSqlRow>>> timeExtractors) {
        super(timeExtractors);
        assert timeExtractors.size() == 1;

        timeExtractor = timeExtractors.get(0).getValue();
        buffer = new PriorityQueue<>(Comparator.comparingLong(timeExtractor));
    }

    @Override
    public void add(JetSqlRow row) {
        buffer.add(row);
    }

    @Override
    public Iterator<JetSqlRow> iterator() {
        return buffer.iterator();
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    @Override
    public Collection<JetSqlRow> content() {
        return buffer;
    }

    @Override
    public void clearExpiredItems(long[] limits, @Nonnull Consumer<JetSqlRow> clearedRowsConsumer) {
        assert limits.length == 1;

        for (JetSqlRow row; (row = buffer.peek()) != null && timeExtractor.applyAsLong(row) < limits[0]; ) {
            clearedRowsConsumer.accept(row);
            buffer.remove();
        }
    }
}
