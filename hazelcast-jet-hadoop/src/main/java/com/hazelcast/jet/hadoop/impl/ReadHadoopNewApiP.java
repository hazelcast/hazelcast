/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.impl.util.Util;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.hadoop.HadoopSources.COPY_ON_READ;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * See {@link HadoopSources#inputFormat}.
 */
public final class ReadHadoopNewApiP<K, V, R> extends AbstractProcessor {

    private static final Class<?>[] EMPTY_ARRAY = new Class[0];

    private final Configuration configuration;
    private final InputFormat inputFormat;
    private final Traverser<R> trav;
    private BiFunctionEx<K, V, R> projectionFn;

    private InternalSerializationService serializationService;
    private RecordReader<K, V> reader;

    private ReadHadoopNewApiP(
            @Nonnull Configuration configuration,
            @Nonnull InputFormat inputFormat,
            @Nonnull List<InputSplit> splits,
            @Nonnull BiFunctionEx<K, V, R> projectionFn
    ) {
        this.configuration = configuration;
        this.inputFormat = inputFormat;
        this.trav = traverseIterable(splits)
                .flatMap(this::traverseSplit);
        this.projectionFn = projectionFn;
    }

    @Override
    protected void init(@Nonnull Context context) {
        HazelcastInstanceImpl instance = (HazelcastInstanceImpl) context.jetInstance().getHazelcastInstance();
        serializationService = instance.getSerializationService();
        // we clone the projection of key/value if configured so because some of the
        // record-readers return the same object for `reader.getCurrentKey()`
        // and `reader.getCurrentValue()` which is mutated for each `reader.nextKeyValue()`.
        if (configuration.getBoolean(COPY_ON_READ, true)) {
            BiFunctionEx<K, V, R> actualProjectionFn = this.projectionFn;
            this.projectionFn = (key, value) -> {
                R result = actualProjectionFn.apply(key, value);
                return result == null ? null : serializationService.toObject(serializationService.toData(result));
            };
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(trav);
    }

    @Override
    public void close() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

    @SuppressWarnings("unchecked")
    private Traverser<R> traverseSplit(InputSplit split) {
        try {
            TaskAttemptContextImpl attemptContext = new TaskAttemptContextImpl(configuration, new TaskAttemptID());
            reader = inputFormat.createRecordReader(split, attemptContext);
            reader.initialize(split, attemptContext);
        } catch (IOException | InterruptedException e) {
            throw sneakyThrow(e);
        }

        return () -> {
            try {
                while (reader.nextKeyValue()) {
                    R projectedRecord = projectionFn.apply(reader.getCurrentKey(), reader.getCurrentValue());
                    if (projectedRecord != null) {
                        return projectedRecord;
                    }
                }
                reader.close();
                return null;
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        };
    }

    private static InputFormat getInputFormat(Configuration configuration) throws Exception {
        Class<?> inputFormatClass = configuration.getClass(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, TextInputFormat.class);
        Constructor<?> constructor = inputFormatClass.getDeclaredConstructor(EMPTY_ARRAY);
        constructor.setAccessible(true);

        InputFormat inputFormat = (InputFormat) constructor.newInstance();
        ReflectionUtils.setConf(inputFormat, configuration);
        return inputFormat;
    }

    public static class MetaSupplier<K, V, R> extends ReadHdfsMetaSupplierBase {

        static final long serialVersionUID = 1L;

        /**
         * The instance is either {@link SerializableConfiguration} or {@link
         * SerializableJobConf}, which are serializable.
         */
        @SuppressFBWarnings("SE_BAD_FIELD")
        private final Configuration configuration;
        private final BiFunctionEx<K, V, R> projectionFn;

        private transient Map<Address, List<IndexedInputSplit>> assigned;

        public MetaSupplier(@Nonnull Configuration configuration, @Nonnull BiFunctionEx<K, V, R> projectionFn) {
            this.configuration = configuration;
            this.projectionFn = projectionFn;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            super.init(context);
            InputFormat inputFormat = getInputFormat(configuration);
            Job job = Job.getInstance(configuration);
            @SuppressWarnings("unchecked")
            List<InputSplit> splits = inputFormat.getSplits(job);
            IndexedInputSplit[] indexedInputSplits = new IndexedInputSplit[splits.size()];
            Arrays.setAll(indexedInputSplits, i -> new IndexedInputSplit(i, splits.get(i)));
            Address[] addrs = context.jetInstance().getCluster().getMembers()
                                     .stream().map(Member::getAddress).toArray(Address[]::new);
            assigned = assignSplitsToMembers(indexedInputSplits, addrs);
            printAssignments(assigned);
        }

        @Nonnull @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address ->
                    new Supplier<>(configuration, assigned.getOrDefault(address, emptyList()), projectionFn);
        }
    }

    private static class Supplier<K, V, R> implements ProcessorSupplier {
        static final long serialVersionUID = 1L;

        /**
         * The instance is either {@link SerializableConfiguration} or {@link
         * SerializableJobConf}, which are serializable.
         */
        @SuppressFBWarnings("SE_BAD_FIELD")
        private Configuration configuration;
        private BiFunctionEx<K, V, R> projectionFn;
        private List<IndexedInputSplit> assignedSplits;

        Supplier(
                Configuration configuration,
                List<IndexedInputSplit> assignedSplits,
                @Nonnull BiFunctionEx<K, V, R> projectionFn
        ) {
            this.configuration = configuration;
            this.projectionFn = projectionFn;
            this.assignedSplits = assignedSplits;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            Map<Integer, List<IndexedInputSplit>> processorToSplits = Util.distributeObjects(count, assignedSplits);
            InputFormat inputFormat = uncheckCall(() -> getInputFormat(configuration));

            return processorToSplits
                    .values().stream()
                    .map(splits -> {
                                List<InputSplit> mappedSplits = splits
                                        .stream()
                                        .map(IndexedInputSplit::getNewSplit)
                                        .collect(toList());
                                return new ReadHadoopNewApiP<>(configuration, inputFormat, mappedSplits, projectionFn);
                            }
                    ).collect(toList());
        }
    }

    /**
     * A {@link ByteArrayOutputStream} that provides an InputStream to read out
     * its current contents.
     */
    private static final class BetterByteArrayOutputStream extends ByteArrayOutputStream {
        private static final int BYTE_MASK = 0xff;

        private int inputStreamPos;
        private InputStream inputStream = new InputStream() {
            @Override
            public int read() {
                return (inputStreamPos < count) ? (buf[inputStreamPos++] & BYTE_MASK) : -1;
            }

            @Override
            public int read(@Nonnull byte[] b, int off, int len) {
                if (inputStreamPos == count) {
                    return -1;
                }
                int copiedLength = Math.min(len, count - inputStreamPos);
                System.arraycopy(buf, inputStreamPos, b, off, copiedLength);
                inputStreamPos += copiedLength;
                return copiedLength;
            }
        };
        private DataInputStream inputStreamDataInput = new DataInputStream(inputStream);

        /**
         * Returns a DataInputStream from which you can read current contents
         * of this output stream. Calling this method again invalidates the
         * previously returned stream.
         */
        DataInputStream getDataInputStream() {
            inputStreamPos = 0;
            return inputStreamDataInput;
        }

        @Override
        public void reset() {
            inputStreamPos = 0;
            super.reset();
        }
    }
}
