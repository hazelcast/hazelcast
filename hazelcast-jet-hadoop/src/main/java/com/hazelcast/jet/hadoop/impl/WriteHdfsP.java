/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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


import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.hadoop.HdfsProcessors;
import com.hazelcast.nio.Address;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static java.lang.String.valueOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.hadoop.mapreduce.TaskType.JOB_SETUP;

/**
 * See {@link HdfsProcessors#writeHdfsP(
 * org.apache.hadoop.mapred.JobConf, FunctionEx, FunctionEx)}.
 */
public final class WriteHdfsP<T, K, V> extends AbstractProcessor {

    private final RecordWriter<K, V> recordWriter;
    private final TaskAttemptContextImpl taskAttemptContext;
    private final OutputCommitter outputCommitter;
    private final FunctionEx<? super T, K> extractKeyFn;
    private final FunctionEx<? super T, V> extractValueFn;

    private WriteHdfsP(RecordWriter<K, V> recordWriter,
                       TaskAttemptContextImpl taskAttemptContext,
                       OutputCommitter outputCommitter,
                       FunctionEx<? super T, K> extractKeyFn,
                       FunctionEx<? super T, V> extractValueFn
    ) {
        this.recordWriter = recordWriter;
        this.taskAttemptContext = taskAttemptContext;
        this.outputCommitter = outputCommitter;
        this.extractKeyFn = extractKeyFn;
        this.extractValueFn = extractValueFn;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        @SuppressWarnings("unchecked")
        T t = (T) item;
        recordWriter.write(extractKeyFn.apply(t), extractValueFn.apply(t));
        return true;
    }

    @Override
    public void close() throws Exception {
        recordWriter.close(Reporter.NULL);
        if (outputCommitter.needsTaskCommit(taskAttemptContext)) {
            outputCommitter.commitTask(taskAttemptContext);
        }
    }

    public static class MetaSupplier<T, K, V> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableJobConf jobConf;
        private final FunctionEx<? super T, K> extractKeyFn;
        private final FunctionEx<? super T, V> extractValueFn;

        private transient OutputCommitter outputCommitter;
        private transient JobContextImpl jobContext;

        public MetaSupplier(SerializableJobConf jobConf,
                            FunctionEx<? super T, K> extractKeyFn,
                            FunctionEx<? super T, V> extractValueFn
        ) {
            this.jobConf = jobConf;
            this.extractKeyFn = extractKeyFn;
            this.extractValueFn = extractValueFn;
        }

        @Override
        public int preferredLocalParallelism() {
            return 2;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            outputCommitter = jobConf.getOutputCommitter();
            jobContext = new JobContextImpl(jobConf, new JobID());
            outputCommitter.setupJob(jobContext);
        }

        @Override
        public void close(Throwable error) throws Exception {
            if (outputCommitter != null && jobContext != null) {
                outputCommitter.commitJob(jobContext);
            }
        }

        @Override @Nonnull
        public FunctionEx<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier<>(jobConf, extractKeyFn, extractValueFn);
        }
    }

    private static class Supplier<T, K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableJobConf jobConf;
        private final FunctionEx<? super T, K> extractKeyFn;
        private final FunctionEx<? super T, V> extractValueFn;

        private transient Context context;
        private transient OutputCommitter outputCommitter;
        private transient JobContextImpl jobContext;

        Supplier(SerializableJobConf jobConf,
                 FunctionEx<? super T, K> extractKeyFn,
                 FunctionEx<? super T, V> extractValueFn
        ) {
            this.jobConf = jobConf;
            this.extractKeyFn = extractKeyFn;
            this.extractValueFn = extractValueFn;
        }

        @Override
        public void init(@Nonnull Context context) {
            this.context = context;
            outputCommitter = jobConf.getOutputCommitter();
            jobContext = new JobContextImpl(jobConf, new JobID());
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return range(0, count).mapToObj(i -> {
                try {
                    String uuid = context.jetInstance().getCluster().getLocalMember().getUuid();
                    TaskAttemptID taskAttemptID = new TaskAttemptID("jet-node-" + uuid, jobContext.getJobID().getId(),
                            JOB_SETUP, i, 0);
                    jobConf.set("mapred.task.id", taskAttemptID.toString());
                    jobConf.setInt("mapred.task.partition", i);

                    TaskAttemptContextImpl taskAttemptContext = new TaskAttemptContextImpl(jobConf, taskAttemptID);
                    @SuppressWarnings("unchecked")
                    OutputFormat<K, V> outFormat = jobConf.getOutputFormat();
                    RecordWriter<K, V> recordWriter = outFormat.getRecordWriter(
                            null, jobConf, uuid + '-' + valueOf(i), Reporter.NULL);
                    return new WriteHdfsP<>(
                            recordWriter, taskAttemptContext, outputCommitter, extractKeyFn, extractValueFn);
                } catch (IOException e) {
                    throw new JetException(e);
                }

            }).collect(toList());
        }
    }
}
