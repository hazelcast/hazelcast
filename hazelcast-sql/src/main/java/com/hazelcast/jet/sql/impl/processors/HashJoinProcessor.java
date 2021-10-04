package com.hazelcast.jet.sql.impl.processors;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.extendArray;

public class HashJoinProcessor implements Processor {

    private JetJoinInfo joinInfo;
    private int rightInputColumnCount;

    private transient Outbox outbox;
    private transient ExpressionEvalContext evalContext;

    private transient Multimap<HashableKeys, Object[]> hashMap;
    private transient boolean rightExhausted;

    public HashJoinProcessor() {
    }

    public HashJoinProcessor(JetJoinInfo joinInfo, int rightInputColumnCount) {
        this.joinInfo = joinInfo;
        this.rightInputColumnCount = rightInputColumnCount;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) throws Exception {
        this.outbox = outbox;
        this.evalContext = SimpleExpressionEvalContext.from(context);
        this.hashMap = LinkedListMultimap.create();
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        switch (ordinal) {
            case 0:
                assert rightExhausted;
                for (Object[] leftRow; (leftRow = (Object[]) inbox.peek()) != null;) {
                    Object[] joinKeys = getHashKeys(leftRow, joinInfo.leftEquiJoinIndices());
                    Collection<Object[]> matchedRows = hashMap.get(new HashableKeys(joinKeys));
                    if (matchedRows.isEmpty()) {
                        if (joinInfo.isLeftOuter()) {
                            if (outbox.offer(extendArray(leftRow, rightInputColumnCount))) {
                                inbox.poll();
                            }
                        } else {
                            inbox.poll();
                        }
                    } else {
                        for (Object[] rightMatchedRow : matchedRows) {
                            Object[] joined = ExpressionUtil.join(leftRow, rightMatchedRow, joinInfo.nonEquiCondition(), evalContext);
                            if (joinInfo.isLeftOuter()) {
                                if (joined == null) {
                                    if (outbox.offer(extendArray(leftRow, rightInputColumnCount))) {
                                        inbox.poll();
                                    }
                                } else {
                                    if (outbox.offer(joined)) {
                                        inbox.poll();
                                    }
                                }
                            } else {
                                if (joined != null) {
                                    if (outbox.offer(joined)) {
                                        inbox.poll();
                                    }
                                } else {
                                    inbox.poll();
                                }
                            }
                        }
                    }
                }
            case 1:
                for (Object[] rightRow; (rightRow = (Object[]) inbox.poll()) != null;) {
                    Object[] joinKeys = getHashKeys(rightRow, joinInfo.rightEquiJoinIndices());
                    hashMap.put(new HashableKeys(joinKeys), rightRow);
                }
                break;
            default:
                throw new IllegalArgumentException("Ordinal must be 0 or 1");
        }
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @Override
    public boolean completeEdge(int ordinal) {
        switch (ordinal) {
            case 0:
                assert rightExhausted;
                return true;
            case 1:
                rightExhausted = true;
                return true;
            default:
                throw new IllegalArgumentException("Ordinal must be 0 or 1");
        }
    }

    private Object[] getHashKeys(Object[] row, int[] joinIndices) {
        Object[] hashKeys = new Object[joinIndices.length];
        for (int i = 0; i < joinIndices.length; i++) {
            hashKeys[i] = row[joinIndices[i]];
        }
        return hashKeys;
    }

    public static HashJoinProcessorSupplier supplier(JetJoinInfo joinInfo, int rightInputColumnCount) {
        return new HashJoinProcessorSupplier(joinInfo, rightInputColumnCount);
    }

    private static class HashableKeys {
        private final Object[] keys;

        private HashableKeys(Object[] keys) {
            this.keys = keys;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HashableKeys that = (HashableKeys) o;
            return Arrays.equals(keys, that.keys);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(keys);
        }
    }

    private static class HashJoinProcessorSupplier implements ProcessorSupplier, DataSerializable {
        private JetJoinInfo joinInfo;
        private int rightInputColumnCount;

        private HashJoinProcessorSupplier() {
        }

        private HashJoinProcessorSupplier(JetJoinInfo joinInfo, int rightInputColumnCount) {
            this.joinInfo = joinInfo;
            this.rightInputColumnCount = rightInputColumnCount;
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            List<HashJoinProcessor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                processors.add(new HashJoinProcessor(joinInfo, rightInputColumnCount));
            }
            return processors;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(joinInfo);
            out.writeInt(rightInputColumnCount);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            joinInfo = in.readObject();
            rightInputColumnCount = in.readInt();
        }
    }
}
