/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector.hadoop;


import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.nio.Address;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.lang.String.valueOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.hadoop.mapreduce.TaskType.JOB_SETUP;

/**
 * See {@link com.hazelcast.jet.core.processor.HdfsProcessors#writeHdfs(
 * org.apache.hadoop.mapred.JobConf, DistributedFunction, DistributedFunction)}.
 */
public final class WriteHdfsP<K, KM, V, VM> extends AbstractProcessor {

    private final RecordWriter<KM, VM> recordWriter;
    private final TaskAttemptContextImpl taskAttemptContext;
    private final OutputCommitter outputCommitter;
    private final DistributedFunction<K, KM> keyMapper;
    private final DistributedFunction<V, VM> valueMapper;

    private WriteHdfsP(RecordWriter<KM, VM> recordWriter, TaskAttemptContextImpl taskAttemptContext,
                       OutputCommitter outputCommitter,
                       DistributedFunction<K, KM> keyMapper, DistributedFunction<V, VM> valueMapper
    ) {
        this.recordWriter = recordWriter;
        this.taskAttemptContext = taskAttemptContext;
        this.outputCommitter = outputCommitter;
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        Map.Entry<K, V> entry = (Map.Entry<K, V>) item;
        recordWriter.write(keyMapper.apply(entry.getKey()), valueMapper.apply(entry.getValue()));
        return true;
    }

    @Override
    public boolean complete() {
        return uncheckCall(() -> {
            recordWriter.close(Reporter.NULL);
            if (outputCommitter.needsTaskCommit(taskAttemptContext)) {
                outputCommitter.commitTask(taskAttemptContext);
            }
            return true;
        });
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    public static class MetaSupplier<K, KM, V, VM> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableJobConf jobConf;
        private final DistributedFunction<K, KM> keyMapper;
        private final DistributedFunction<V, VM> valueMapper;

        private transient Address address;

        public MetaSupplier(SerializableJobConf jobConf,
                            DistributedFunction<K, KM> keyMapper,
                            DistributedFunction<V, VM> valueMapper
        ) {
            this.jobConf = jobConf;
            this.keyMapper = keyMapper;
            this.valueMapper = valueMapper;
        }

        @Override
        public void init(@Nonnull Context context) {
            address = context.jetInstance().getCluster().getLocalMember().getAddress();
        }

        @Override @Nonnull
        public DistributedFunction<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier<>(address.equals(this.address), jobConf, keyMapper, valueMapper);
        }
    }

    private static class Supplier<K, KM, V, VM> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final boolean commitJob;
        private final SerializableJobConf jobConf;
        private final DistributedFunction<K, KM> keyMapper;
        private final DistributedFunction<V, VM> valueMapper;

        private transient Context context;
        private transient OutputCommitter outputCommitter;
        private transient JobContextImpl jobContext;

        Supplier(boolean commitJob,
                 SerializableJobConf jobConf,
                 DistributedFunction<K, KM> keyMapper,
                 DistributedFunction<V, VM> valueMapper
        ) {
            this.commitJob = commitJob;
            this.jobConf = jobConf;
            this.keyMapper = keyMapper;
            this.valueMapper = valueMapper;
        }

        @Override
        public void init(@Nonnull Context context) {
            this.context = context;
            outputCommitter = jobConf.getOutputCommitter();
            jobContext = new JobContextImpl(jobConf, new JobID());
        }

        @Override
        public void complete(Throwable error) {
            if (commitJob) {
                uncheckRun(() -> outputCommitter.commitJob(jobContext));
            }
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return range(0, count).mapToObj(i -> {
                if (i == 0) {
                    uncheckCall(() -> {
                        outputCommitter.setupJob(jobContext);
                        return null;
                    });
                }
                String uuid = context.jetInstance().getCluster().getLocalMember().getUuid();
                TaskAttemptID taskAttemptID = new TaskAttemptID("jet-node-" + uuid, jobContext.getJobID().getId(),
                        JOB_SETUP, i, 0);
                jobConf.set("mapred.task.id", taskAttemptID.toString());
                jobConf.setInt("mapred.task.partition", i);

                TaskAttemptContextImpl taskAttemptContext = new TaskAttemptContextImpl(jobConf, taskAttemptID);
                RecordWriter recordWriter = uncheckCall(() ->
                        jobConf.getOutputFormat()
                               .getRecordWriter(null,
                                       jobConf, uuid + '-' + valueOf(i), Reporter.NULL));
                return new WriteHdfsP(recordWriter, taskAttemptContext, outputCommitter, keyMapper, valueMapper);

            }).collect(toList());
        }
    }
}
