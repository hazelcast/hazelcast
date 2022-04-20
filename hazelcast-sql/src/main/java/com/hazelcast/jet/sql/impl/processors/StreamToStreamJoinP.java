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
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class StreamToStreamJoinP extends AbstractProcessor {
    // TODO: replace with ExpressionEvalContext.from(context) in init() method.
    private static final ExpressionEvalContext MOCK_EEC =
            new ExpressionEvalContext(emptyList(), new DefaultSerializationServiceBuilder().build());
    /**
     * <p>
     * JOIN condition should be transformed into such form:
     * <pre>
     *  l.time >= r.time - constant1
     *  r.time >= l.time - constant2
     * </pre>
     */
    private final JetJoinInfo joinInfo;
    private final Map<Byte, ToLongFunctionEx<JetSqlRow>> leftTimeExtractor;
    private final Map<Byte, ToLongFunctionEx<JetSqlRow>> rightTimeExtractor;
    private final Map<Byte, Map<Byte, Long>> postponeTimeMap;

    private final long[][] wmState;

    private Iterator<JetSqlRow> pos;

    private JetSqlRow currItem;
    private final List<JetSqlRow>[] buffer = new List[]{new LinkedList<>(), new LinkedList<>()};
    private final Queue<JetSqlRow> pendingOutput = new ArrayDeque<>();
    private final Queue<Watermark> pendingWatermarks = new ArrayDeque<>();

    public StreamToStreamJoinP(
            final JetJoinInfo joinInfo,
            final Map<Byte, ToLongFunctionEx<JetSqlRow>> leftTimeExtractor,
            final Map<Byte, ToLongFunctionEx<JetSqlRow>> rightTimeExtractor,
            final Map<Byte, Map<Byte, Long>> postponeTimeMap
    ) {
        this.joinInfo = joinInfo;
        this.leftTimeExtractor = leftTimeExtractor;
        this.rightTimeExtractor = rightTimeExtractor;
        this.postponeTimeMap = postponeTimeMap;

        this.wmState = new long[postponeTimeMap.size()][postponeTimeMap.size()];
        for (byte i = 0; i < postponeTimeMap.size(); ++i) {
            for (byte j = 0; j < postponeTimeMap.size(); ++j) {
                if (postponeTimeMap.get(i).get(j) == null) {
                    wmState[i][j] = Long.MAX_VALUE;
                } else {
                    wmState[i][j] = Long.MIN_VALUE;
                }
            }
        }
    }

    @Override
    public boolean tryProcess(int ordinal, @Nonnull Object item) {
        while (!pendingOutput.isEmpty()) {
            if (!tryEmit(pendingOutput.peek())) {
                return false;
            } else {
                pendingOutput.remove();
            }
        }

        //  having side input, traverse opposite buffer
        if (currItem == null) {
            currItem = (JetSqlRow) item;
            buffer[ordinal].add(currItem);
            pos = buffer[1 - ordinal].iterator();

            JetSqlRow joinedRow = null;
            // If current join type is LEFT/RIGHT and opposite buffer is empty,
            // we should to produce input row with null-filled opposite side.
            if (!pos.hasNext() && !joinInfo.isInner()) {
                if (ordinal == 1 && joinInfo.isLeftOuter()) {
                    // fill LEFT side with nulls
                    joinedRow = ExpressionUtil.join(
                            new JetSqlRow(currItem.getSerializationService(), new Object[]{null}),
                            currItem,
                            joinInfo.condition(),
                            MOCK_EEC
                    );
                } else if (ordinal == 0 && joinInfo.isRightOuter()) {
                    // fill RIGHT side with nulls
                    joinedRow = ExpressionUtil.join(
                            currItem,
                            new JetSqlRow(currItem.getSerializationService(), new Object[]{null}),
                            joinInfo.condition(),
                            MOCK_EEC
                    );
                }
            }

            if (joinedRow != null) {
                pendingOutput.offer(joinedRow);
            }
            return false;
        }

        if (!pos.hasNext()) {
            pos = null;
            currItem = null;
            return true;
        }

        JetSqlRow preparedOutput = ExpressionUtil.join(
                ordinal == 0 ? currItem : pos.next(),
                ordinal == 0 ? pos.next() : currItem,
                joinInfo.condition(),
                MOCK_EEC
        );

        if (preparedOutput == null || !tryEmit(preparedOutput)) {
            pendingOutput.offer(preparedOutput);
        }
        return false;
    }

    @Override
    public boolean tryProcessWatermark(int ordinal, @Nonnull Watermark watermark) {
        // if pending watermarks available - try to send them
        if (!pendingWatermarks.isEmpty()) {
            while (!pendingWatermarks.isEmpty()) {
                Watermark wm = pendingWatermarks.peek();
                if (!tryEmit(wm)) {
                    return false;
                } else {
                    pendingWatermarks.remove();
                }
            }
            return false;
        }

        // update wm state
        offer(watermark);

        // try to clear buffers if possible
        clearExpiredItemsInBuffer(ordinal, watermark);

        // We can't immediately emit current WM, as it would render items in left buffer late.
        // Instead, we can emit WM with the minimum available time for this WM key.
        long minimumItemTime = findMinimumBufferTime(ordinal, watermark);
        if (minimumItemTime != Long.MAX_VALUE) {
            Watermark wm = new Watermark(minimumItemTime, watermark.key());
            if (!tryEmit(wm)) {
                pendingWatermarks.offer(wm);
            }
        }

        return true;
    }

    private void offer(Watermark watermark) {
        byte key = watermark.key();
        final Map<Byte, Long> wmKeyMapping = postponeTimeMap.get(key);
        for (byte i : wmKeyMapping.keySet()) {
            if (wmState[i][key] != Long.MIN_VALUE) {
                wmState[i][key] = Math.min(watermark.timestamp() - wmKeyMapping.get(i), wmState[i][key]);
            } else {
                wmState[i][key] = watermark.timestamp() - wmKeyMapping.get(i);
            }
        }
    }

    private long findMinimumBufferTime(int ordinal, Watermark watermark) {
        byte key = watermark.key();
        ToLongFunctionEx<JetSqlRow> extractor = ordinal == 0 ? leftTimeExtractor.get(key) : rightTimeExtractor.get(key);

        long min = Long.MAX_VALUE;
        for (JetSqlRow row : buffer[ordinal]) {
            min = Math.min(min, extractor.applyAsLong(row));
        }
        return min;
    }

    private long findMinimumGroupTime(byte group) {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < wmState[group].length; ++i) {
            min = Math.min(min, wmState[group][i]);
        }
        return min;
    }

    private void clearExpiredItemsInBuffer(int ordinal, Watermark wm) {
        final ToLongFunctionEx<JetSqlRow> tsExtractor = ordinal == 0
                ? leftTimeExtractor.get(wm.key())
                : rightTimeExtractor.get(wm.key());

        long limit = findMinimumGroupTime(wm.key());
        if (limit == Long.MAX_VALUE || limit == Long.MIN_VALUE) {
            return;
        }

        buffer[ordinal] = buffer[ordinal]
                .stream()
                .filter(row -> tsExtractor.applyAsLong(row) >= limit)
                .collect(toList());
    }
}