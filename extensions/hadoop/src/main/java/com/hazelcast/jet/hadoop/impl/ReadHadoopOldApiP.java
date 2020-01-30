/*
 * Copyright 2020 Hazelcast Inc.
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

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.impl.util.Util;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.mapred.Reporter.NULL;

/**
 * See {@link HadoopSources#inputFormat}.
 */
public final class ReadHadoopOldApiP<K, V, R> extends AbstractProcessor {

    private final Traverser<R> trav;
    private final BiFunctionEx<K, V, R> projectionFn;

    private ReadHadoopOldApiP(@Nonnull List<RecordReader> recordReaders, @Nonnull BiFunctionEx<K, V, R> projectionFn) {
        this.trav = traverseIterable(recordReaders).flatMap(this::traverseRecordReader);
        this.projectionFn = projectionFn;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(trav);
    }

    private Traverser<R> traverseRecordReader(RecordReader<K, V> r) {
        return () -> {
            K key = r.createKey();
            V value = r.createValue();
            try {
                while (r.next(key, value)) {
                    R projectedRecord = projectionFn.apply(key, value);
                    if (projectedRecord != null) {
                        return projectedRecord;
                    }
                }
                r.close();
                return null;
            } catch (IOException e) {
                throw sneakyThrow(e);
            }
        };
    }

    public static class MetaSupplier<K, V, R> extends ReadHdfsMetaSupplierBase {

        static final long serialVersionUID = 1L;

        @SuppressFBWarnings("SE_BAD_FIELD")
        private final JobConf jobConf;
        private final BiFunctionEx<K, V, R> projectionFn;

        private transient Map<Address, List<IndexedInputSplit>> assigned;

        public MetaSupplier(@Nonnull JobConf jobConf, @Nonnull BiFunctionEx<K, V, R> projectionFn) {
            this.jobConf = jobConf;
            this.projectionFn = projectionFn;
        }

        @Override
        public int preferredLocalParallelism() {
            return 2;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            super.init(context);
            int totalParallelism = context.totalParallelism();
            InputFormat inputFormat = jobConf.getInputFormat();
            InputSplit[] splits = inputFormat.getSplits(jobConf, totalParallelism);
            IndexedInputSplit[] indexedInputSplits = new IndexedInputSplit[splits.length];
            Arrays.setAll(indexedInputSplits, i -> new IndexedInputSplit(i, splits[i]));

            Address[] addrs = context.jetInstance().getCluster().getMembers()
                                     .stream().map(Member::getAddress).toArray(Address[]::new);
            assigned = assignSplitsToMembers(indexedInputSplits, addrs);
            printAssignments(assigned);
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address ->
                    new Supplier<>(jobConf, assigned.getOrDefault(address, emptyList()), projectionFn);
        }
    }

    private static class Supplier<K, V, R> implements ProcessorSupplier {
        static final long serialVersionUID = 1L;

        @SuppressFBWarnings("SE_BAD_FIELD")
        private JobConf jobConf;
        private BiFunctionEx<K, V, R> projectionFn;
        private List<IndexedInputSplit> assignedSplits;

        Supplier(JobConf jobConf, List<IndexedInputSplit> assignedSplits, @Nonnull BiFunctionEx<K, V, R> projectionFn) {
            this.jobConf = jobConf;
            this.projectionFn = projectionFn;
            this.assignedSplits = assignedSplits;
        }

        @Override
        @Nonnull
        public List<Processor> get(int count) {
            Map<Integer, List<IndexedInputSplit>> processorToSplits = Util.distributeObjects(count, assignedSplits);
            InputFormat inputFormat = jobConf.getInputFormat();
            Processor noopProcessor = Processors.noopP().get();

            return processorToSplits
                    .values().stream()
                    .map(splits -> splits.isEmpty()
                            ? noopProcessor
                            : new ReadHadoopOldApiP<>(splits.stream()
                                                            .map(IndexedInputSplit::getOldSplit)
                                                            .map(split -> uncheckCall(() ->
                                                                  inputFormat.getRecordReader(split, jobConf, NULL)))
                                                            .collect(toList()), projectionFn)
                    ).collect(toList());
        }
    }
}
