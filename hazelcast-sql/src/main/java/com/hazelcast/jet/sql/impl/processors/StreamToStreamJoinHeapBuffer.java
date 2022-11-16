/*
 * Copyright 2021 Hazelcast Inc.
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
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

public class StreamToStreamJoinHeapBuffer extends IStreamToStreamJoinBuffer {
    private final PriorityQueue<JetSqlRow> buffer;
    private final ToLongFunctionEx<JetSqlRow> timeExtractor;

    public StreamToStreamJoinHeapBuffer(
            JetJoinInfo joinInfo,
            boolean isLeftInput,
            List<Map.Entry<Byte, ToLongFunctionEx<JetSqlRow>>> timeExtractors) {
        super(joinInfo, isLeftInput, timeExtractors);
        assert timeExtractors.size() == 1;

        timeExtractor = timeExtractors.iterator().next().getValue();

        Comparator<JetSqlRow> comparator = (row1, row2) -> {
            ToLongFunctionEx<JetSqlRow> extractor = timeExtractor;
            return Long.compare(extractor.applyAsLong(row1), extractor.applyAsLong(row2));
        };

        this.buffer = new PriorityQueue<>(comparator);
    }

    @Override
    public void add(JetSqlRow row) {
        buffer.offer(row);
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
    public long[] clearExpiredItems(
            long[] limits,
            @Nonnull Set<JetSqlRow> unusedEventsTracker,
            @Nonnull Queue<Object> pendingOutput,
            @Nonnull ExpressionEvalContext eec) {
        assert limits.length == 1;

        JetSqlRow row = buffer.peek();
        while (row != null && timeExtractor.applyAsLong(row) < limits[0]) {
            if (shouldProduceNullRow && unusedEventsTracker.remove(row)) {
                // 5.4 : If doing an outer join, emit events removed from the buffer,
                // with `null`s for the other side, if the event was never joined.
                JetSqlRow joinedRow = composeRowWithNulls(row, eec);
                if (joinedRow != null) {
                    pendingOutput.add(joinedRow);
                }
            }
            buffer.poll();
            row = buffer.peek();
        }

        if (buffer.size() > 0) {
            return new long[]{timeExtractor.applyAsLong(buffer.element())};
        } else {
            return new long[]{Long.MAX_VALUE};
        }
    }
}
