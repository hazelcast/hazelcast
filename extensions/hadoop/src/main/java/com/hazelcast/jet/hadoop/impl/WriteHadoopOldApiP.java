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

package com.hazelcast.jet.hadoop.impl;


import com.hazelcast.cluster.Address;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.hadoop.HadoopSinks;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.mapred.JobConf;
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

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.hadoop.mapreduce.TaskType.JOB_SETUP;

/**
 * See {@link HadoopSinks#outputFormat}.
 */
public final class WriteHadoopOldApiP<T, K, V> extends AbstractProcessor {

    private final RecordWriter<K, V> recordWriter;
    private final TaskAttemptContextImpl taskAttemptContext;
    private final OutputCommitter outputCommitter;
    private final FunctionEx<? super T, K> extractKeyFn;
    private final FunctionEx<? super T, V> extractValueFn;

    private WriteHadoopOldApiP(RecordWriter<K, V> recordWriter,
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

        @SuppressFBWarnings("SE_BAD_FIELD")
        private final JobConf jobConf;
        private final FunctionEx<? super T, K> extractKeyFn;
        private final FunctionEx<? super T, V> extractValueFn;

        private transient OutputCommitter outputCommitter;
        private transient JobContextImpl jobContext;

        public MetaSupplier(JobConf jobConf,
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

        @SuppressFBWarnings("SE_BAD_FIELD")
        private final JobConf jobConf;
        private final FunctionEx<? super T, K> extractKeyFn;
        private final FunctionEx<? super T, V> extractValueFn;

        private transient Context context;
        private transient OutputCommitter outputCommitter;
        private transient JobContextImpl jobContext;

        Supplier(JobConf jobConf,
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
                    String uuid = context.hazelcastInstance().getCluster().getLocalMember().getUuid().toString();
                    TaskAttemptID taskAttemptID = new TaskAttemptID("jet-node-" + uuid, jobContext.getJobID().getId(),
                            JOB_SETUP, i, 0);

                    JobConf copiedConfig = new JobConf(jobConf);

                    copiedConfig.set("mapred.task.id", taskAttemptID.toString());
                    copiedConfig.setInt("mapred.task.partition", i);

                    TaskAttemptContextImpl taskAttemptContext = new TaskAttemptContextImpl(copiedConfig, taskAttemptID);
                    @SuppressWarnings("unchecked")
                    OutputFormat<K, V> outFormat = copiedConfig.getOutputFormat();
                    RecordWriter<K, V> recordWriter = outFormat.getRecordWriter(
                            null, copiedConfig, uuid + '-' + i, Reporter.NULL);
                    return new WriteHadoopOldApiP<>(
                            recordWriter, taskAttemptContext, outputCommitter, extractKeyFn, extractValueFn);
                } catch (IOException e) {
                    throw new JetException(e);
                }

            }).collect(toList());
        }
    }
}
