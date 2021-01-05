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
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.file.impl.FileTraverser;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.mapred.Reporter.NULL;

/**
 * See {@link HadoopSources#inputFormat}.
 */
public final class ReadHadoopOldApiP<K, V, R> extends AbstractProcessor {

    private final HadoopFileTraverser<K, V, R> traverser;

    private ReadHadoopOldApiP(
            @Nonnull JobConf jobConf,
            @Nonnull List<InputSplit> splits,
            @Nonnull BiFunctionEx<K, V, R> projectionFn
    ) {
        this.traverser = new HadoopFileTraverser<>(jobConf, splits, projectionFn);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    @Override
    public void close() throws Exception {
        traverser.close();
    }

    public static class MetaSupplier<K, V, R> extends ReadHdfsMetaSupplierBase<R> {

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
        public void init(@Nonnull Context context) throws Exception {
            super.init(context);
            if (shouldSplitOnMembers(jobConf)) {
                assigned = new HashMap<>();
            } else {
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
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier<>(jobConf, assigned.getOrDefault(address, emptyList()), projectionFn);
        }

        @Override
        public FileTraverser<R> traverser() throws Exception {
            InputFormat inputFormat = jobConf.getInputFormat();
            return new HadoopFileTraverser<>(jobConf, asList(inputFormat.getSplits(jobConf, 1)), projectionFn);
        }
    }

    private static class Supplier<K, V, R> implements ProcessorSupplier {
        static final long serialVersionUID = 1L;

        @SuppressFBWarnings("SE_BAD_FIELD")
        private final JobConf jobConf;
        private final BiFunctionEx<K, V, R> projectionFn;
        private final List<IndexedInputSplit> assignedSplits;

        Supplier(JobConf jobConf, List<IndexedInputSplit> assignedSplits, @Nonnull BiFunctionEx<K, V, R> projectionFn) {
            this.jobConf = jobConf;
            this.projectionFn = projectionFn;
            this.assignedSplits = assignedSplits;
        }

        @Override
        @Nonnull
        public List<Processor> get(int count) {
            List<InputSplit> inputSplits;
            if (shouldSplitOnMembers(jobConf)) {
                inputSplits = uncheckCall(() -> asList(jobConf.getInputFormat().getSplits(jobConf, count)));
            } else {
                inputSplits = assignedSplits.stream().map(IndexedInputSplit::getOldSplit).collect(toList());
            }
            return Util.distributeObjects(count, inputSplits)
                    .values().stream()
                    .map(splits -> new ReadHadoopOldApiP<>(jobConf, splits, projectionFn))
                    .collect(toList());
        }
    }

    /**
     * If all the input paths are of LocalFileSystem and not marked as shared
     * (see {@link HadoopSources#SHARED_LOCAL_FS}), split the input paths on
     * members.
     */
    private static boolean shouldSplitOnMembers(JobConf jobConf) {
        // If the local file system is marked as shared, don't split on members
        if (jobConf.getBoolean(HadoopSources.SHARED_LOCAL_FS, false)) {
            return false;
        }
        // Local file system is not marked as shared, throw exception if
        // there are local file system and remote file system in the inputs.
        Path[] inputPaths = FileInputFormat.getInputPaths(jobConf);
        boolean hasLocalFileSystem = false;
        boolean hasRemoteFileSystem = false;
        for (Path inputPath : inputPaths) {
            if (isLocalFileSystem(inputPath, jobConf)) {
                hasLocalFileSystem = true;
            } else {
                hasRemoteFileSystem = true;
            }
        }
        if (hasLocalFileSystem && hasRemoteFileSystem) {
            throw new IllegalArgumentException(
                    "LocalFileSystem should be marked as shared when used with other remote file systems");
        }
        return hasLocalFileSystem;
    }

    private static boolean isLocalFileSystem(Path inputPath, JobConf jobConf) {
        FileSystem fileSystem = uncheckCall(() -> inputPath.getFileSystem(jobConf));
        return fileSystem instanceof LocalFileSystem || fileSystem instanceof RawLocalFileSystem;
    }

    private static final class HadoopFileTraverser<K, V, R> implements FileTraverser<R> {

        private final JobConf jobConf;
        private final InputFormat<K, V> inputFormat;
        private final BiFunctionEx<K, V, R> projectionFn;
        private final Traverser<R> delegate;

        private RecordReader<K, V> reader;

        private HadoopFileTraverser(
                JobConf jobConf,
                List<InputSplit> splits,
                BiFunctionEx<K, V, R> projectionFn
        ) {
            this.jobConf = jobConf;
            this.inputFormat = jobConf.getInputFormat();
            this.projectionFn = projectionFn;
            this.delegate = traverseIterable(splits).flatMap(this::traverseSplit);
        }

        private Traverser<R> traverseSplit(InputSplit inputSplit) {
            reader = uncheckCall(() -> inputFormat.getRecordReader(inputSplit, jobConf, NULL));

            return () -> uncheckCall(() -> {
                K key = reader.createKey();
                V value = reader.createValue();
                while (reader.next(key, value)) {
                    R projectedRecord = projectionFn.apply(key, value);
                    if (projectedRecord != null) {
                        return projectedRecord;
                    }
                }
                reader.close();
                return null;
            });
        }

        @Override
        public R next() {
            return delegate.next();
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
