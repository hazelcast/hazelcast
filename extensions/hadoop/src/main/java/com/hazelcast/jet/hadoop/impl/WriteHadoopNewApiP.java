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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.hadoop.mapreduce.TaskType.JOB_SETUP;

/**
 * See {@link HadoopSinks#outputFormat}.
 */
public final class WriteHadoopNewApiP<T, K, V> extends AbstractProcessor {

    private final RecordWriter<K, V> recordWriter;
    private final TaskAttemptContextImpl taskAttemptContext;
    private final OutputCommitter outputCommitter;
    private final FunctionEx<? super T, K> extractKeyFn;
    private final FunctionEx<? super T, V> extractValueFn;

    private WriteHadoopNewApiP(RecordWriter<K, V> recordWriter,
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
        recordWriter.close(taskAttemptContext);
        if (outputCommitter.needsTaskCommit(taskAttemptContext)) {
            outputCommitter.commitTask(taskAttemptContext);
        }
    }

    public static class MetaSupplier<T, K, V> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        @SuppressFBWarnings("SE_BAD_FIELD")
        private final Configuration configuration;
        private final FunctionEx<? super T, K> extractKeyFn;
        private final FunctionEx<? super T, V> extractValueFn;

        private transient OutputCommitter outputCommitter;
        private transient JobContextImpl jobContext;

        public MetaSupplier(Configuration configuration,
                            FunctionEx<? super T, K> extractKeyFn,
                            FunctionEx<? super T, V> extractValueFn
        ) {
            this.configuration = configuration;
            this.extractKeyFn = extractKeyFn;
            this.extractValueFn = extractValueFn;
        }

        @Override
        public int preferredLocalParallelism() {
            return 2;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            jobContext = new JobContextImpl(configuration, new JobID());
            OutputFormat outputFormat = getOutputFormat(configuration);

            outputCommitter = outputFormat.getOutputCommitter(getTaskAttemptContext(configuration, jobContext,
                    getUuid(context)));
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
            return address -> new Supplier<>(configuration, extractKeyFn, extractValueFn);
        }
    }

    private static class Supplier<T, K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        @SuppressFBWarnings("SE_BAD_FIELD")
        private final Configuration configuration;
        private final FunctionEx<? super T, K> extractKeyFn;
        private final FunctionEx<? super T, V> extractValueFn;

        private transient Context context;
        private transient OutputCommitter outputCommitter;
        private transient JobContextImpl jobContext;

        Supplier(Configuration configuration,
                 FunctionEx<? super T, K> extractKeyFn,
                 FunctionEx<? super T, V> extractValueFn
        ) {
            this.configuration = configuration;
            this.extractKeyFn = extractKeyFn;
            this.extractValueFn = extractValueFn;
        }

        @Override
        public void init(@Nonnull Context context) throws IOException, InterruptedException {
            this.context = context;

            jobContext = new JobContextImpl(configuration, new JobID());
            OutputFormat outputFormat = getOutputFormat(configuration);
            outputCommitter = outputFormat.getOutputCommitter(getTaskAttemptContext(configuration, jobContext,
                    getUuid(context)));
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return range(0, count).mapToObj(localIndex -> {
                try {
                    JobConf copiedConfig = new JobConf(configuration);
                    int globalIndex = context.memberIndex() * context.localParallelism() + localIndex;
                    TaskAttemptID taskAttemptID = getTaskAttemptID(globalIndex, jobContext, getUuid(context));
                    copiedConfig.set("mapreduce.task.attempt.id", taskAttemptID.toString());
                    copiedConfig.setInt("mapreduce.task.partition", globalIndex);

                    TaskAttemptContextImpl taskAttemptContext = new TaskAttemptContextImpl(copiedConfig, taskAttemptID);
                    @SuppressWarnings("unchecked")
                    OutputFormat<K, V> outFormat = getOutputFormat(copiedConfig);
                    RecordWriter<K, V> recordWriter = outFormat.getRecordWriter(taskAttemptContext);
                    return new WriteHadoopNewApiP<>(recordWriter, taskAttemptContext, outputCommitter, extractKeyFn,
                            extractValueFn);
                } catch (Exception e) {
                    throw new JetException(e);
                }

            }).collect(toList());
        }
    }

    private static String getUuid(@Nonnull ProcessorMetaSupplier.Context context) {
        return context.hazelcastInstance().getCluster().getLocalMember().getUuid().toString();
    }

    private static OutputFormat getOutputFormat(Configuration config) {
        Class<?> outputFormatClass = config.getClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, TextOutputFormat.class);
        return (OutputFormat) ReflectionUtils.newInstance(outputFormatClass, config);
    }

    private static TaskAttemptContextImpl getTaskAttemptContext(Configuration jobConf,
                                                                JobContextImpl jobContext, String uuid) {
        return new TaskAttemptContextImpl(jobConf, getTaskAttemptID(0, jobContext, uuid));
    }

    private static TaskAttemptID getTaskAttemptID(int id, JobContextImpl jobContext, String uuid) {
        return new TaskAttemptID("jet-node-" + uuid, jobContext.getJobID().getId(), JOB_SETUP, id, 0);
    }
}
