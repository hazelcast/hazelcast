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
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public abstract class StreamToStreamJoinBuffer implements Iterable<JetSqlRow> {
    protected final JetJoinInfo joinInfo;
    protected final boolean isLeft;
    protected final boolean shouldProduceNullRow;

    protected final List<Map.Entry<Byte, ToLongFunctionEx<JetSqlRow>>> timeExtractors;

    protected JetSqlRow emptyRow;

    public StreamToStreamJoinBuffer(
            JetJoinInfo joinInfo,
            boolean isLeftInput,
            List<Map.Entry<Byte, ToLongFunctionEx<JetSqlRow>>> timeExtractors
    ) {
        this.joinInfo = joinInfo;
        this.isLeft = isLeftInput;
        this.shouldProduceNullRow = (isLeftInput && joinInfo.isLeftOuter()) || (!isLeftInput && joinInfo.isRightOuter());
        this.timeExtractors = timeExtractors;
    }

    public void init(JetSqlRow emptyRow) {
        this.emptyRow = emptyRow;
    }

    public abstract void add(JetSqlRow row);

    public abstract Iterator<JetSqlRow> iterator();

    public abstract int size();

    public abstract boolean isEmpty();

    // for testing purposes only
    abstract Collection<JetSqlRow> content();

    /**
     * Clears expired items in current buffer, and returns a new minimums time array.
     *
     * @param limits              array of limits for
     * @param unusedEventsTracker set of unused events, method will produce null-filled row in case if JOIN is OUTER
     * @param pendingOutput       queue of joined rows, is used only if JOIN is OUTER.
     * @param eec                 Jet's expression evaluation context
     * @return a new minimums time array.
     */
    public abstract long[] clearExpiredItems(long[] limits,
                                             @Nonnull Set<JetSqlRow> unusedEventsTracker,
                                             @Nonnull Queue<Object> pendingOutput,
                                             @Nonnull ExpressionEvalContext eec);

    /**
     * Produces null-filled row in case of OUTER JOIN :
     * <ul>
     * <li>Fills the <b>left</b> side with nulls if JOIN is RIGHT OUTER</li>
     * <li>Fills the <b>right</b> side with nulls if JOIN is LEFT OUTER</li>
     * </ul>
     */
    protected JetSqlRow composeRowWithNulls(JetSqlRow row, ExpressionEvalContext eec) {
        return ExpressionUtil.join(
                isLeft ? row : emptyRow,
                isLeft ? emptyRow : row,
                ConstantExpression.TRUE,
                eec);
    }
}
