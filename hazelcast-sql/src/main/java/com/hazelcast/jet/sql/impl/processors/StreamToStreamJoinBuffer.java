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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

abstract class StreamToStreamJoinBuffer implements Iterable<JetSqlRow> {
    protected final List<Map.Entry<Byte, ToLongFunctionEx<JetSqlRow>>> timeExtractors;

    StreamToStreamJoinBuffer(List<Map.Entry<Byte, ToLongFunctionEx<JetSqlRow>>> timeExtractors) {
        this.timeExtractors = timeExtractors;
    }

    public abstract void add(JetSqlRow row);

    public abstract Iterator<JetSqlRow> iterator();

    public abstract int size();

    public abstract boolean isEmpty();

    abstract Collection<JetSqlRow> content();

    /**
     * Clears expired items in current buffer, and returns a new minimums time array.
     *
     * @param limits array of limits for
     */
    public abstract void clearExpiredItems(long[] limits, @Nonnull Consumer<JetSqlRow> clearedRowsConsumer);
}
