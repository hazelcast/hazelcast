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

package com.hazelcast.jet.connector.hadoop;


import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.nio.Address;
import org.apache.hadoop.mapred.JobConf;
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

import static com.hazelcast.jet.Distributed.Function.identity;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.lang.String.valueOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.hadoop.mapreduce.TaskType.JOB_SETUP;

/**
 * HDFS writer for Jet, consumes Map.Entry objects and writes them to the output file in HDFS.
 */
public final class WriteHdfsP extends AbstractProcessor {

    private final RecordWriter recordWriter;
    private final TaskAttemptContextImpl taskAttemptContext;
    private final OutputCommitter outputCommitter;
    private final Function keyMapper;
    private final Function valueMapper;

    private WriteHdfsP(RecordWriter recordWriter, TaskAttemptContextImpl taskAttemptContext,
                       OutputCommitter outputCommitter, Function keyMapper, Function valueMapper) {
        this.recordWriter = recordWriter;
        this.taskAttemptContext = taskAttemptContext;
        this.outputCommitter = outputCommitter;
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        Map.Entry entry = (Map.Entry) item;
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

    /**
     * Returns a meta-supplier of processors that write HDFS files.
     *
     * @param jobConf JobConf for writing files with the appropriate output format and path
     * @return {@link ProcessorMetaSupplier} supplier
     */
    public static ProcessorMetaSupplier writeHdfs(JobConf jobConf) {
        return new MetaSupplier(jobConf, identity(), identity());
    }

    /**
     * Returns a meta-supplier of processors that write HDFS files.
     *
     * <p>
     * For some output formats (eg {@code SequenceFileOutputFormat}) key/value pairs should be {@code Writable}
     * One can use key/value mappers for converting entries to {@code Writable}
     * </p>
     *
     * @param jobConf     JobConf for writing files with the appropriate output format and path
     * @param <K>     key type of the records
     * @param <KR>     the type of the mapped key
     * @param keyMapper   mapper which can be used to map the key to another value
     * @param <V>     value type of the records
     * @param <VR>     the type of the mapped value
     * @param valueMapper mapper which can be used to map the value to another value
     * @return {@link ProcessorMetaSupplier} supplier
     */
    public static <K, V, KR, VR> ProcessorMetaSupplier writeHdfs(JobConf jobConf,
                                                               @Nonnull Function<K, KR> keyMapper,
                                                               @Nonnull Function<V, VR> valueMapper) {
        return new MetaSupplier(jobConf, keyMapper, valueMapper);
    }


    private static class MetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final JobConfiguration configuration;
        private final Function keyMapper;
        private final Function valueMapper;

        private transient Address address;

        MetaSupplier(JobConf jobConf, Function keyMapper, Function valueMapper) {
            this.configuration = new JobConfiguration(jobConf);
            this.keyMapper = keyMapper;
            this.valueMapper = valueMapper;
        }

        @Override
        public void init(@Nonnull Context context) {
            address = context.jetInstance().getCluster().getLocalMember().getAddress();
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier(address.equals(this.address), configuration, keyMapper, valueMapper);
        }
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private boolean commitJob;
        private JobConfiguration configuration;
        private Function keyMapper;
        private Function valueMapper;

        private transient Context context;
        private transient OutputCommitter outputCommitter;
        private transient JobID jobId;
        private transient JobContextImpl jobContext;

        Supplier(boolean commitJob, JobConfiguration configuration, Function keyMapper, Function valueMapper) {
            this.commitJob = commitJob;
            this.configuration = configuration;
            this.keyMapper = keyMapper;
            this.valueMapper = valueMapper;
        }

        @Override
        public void init(@Nonnull Context context) {
            this.context = context;
            outputCommitter = configuration.getOutputCommitter();
            jobId = new JobID();
            jobContext = new JobContextImpl(configuration, jobId);
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
                TaskAttemptID taskAttemptID = new TaskAttemptID("jet-node-" + uuid, jobId.getId(),
                        JOB_SETUP, i, 0);
                configuration.set("mapred.task.id", taskAttemptID.toString());
                configuration.setInt("mapred.task.partition", i);

                TaskAttemptContextImpl taskAttemptContext = new TaskAttemptContextImpl(configuration, taskAttemptID);
                RecordWriter recordWriter = uncheckCall(() -> configuration.getOutputFormat().getRecordWriter(null,
                        configuration, uuid + '-' + valueOf(i), Reporter.NULL));
                return new WriteHdfsP(recordWriter, taskAttemptContext, outputCommitter, keyMapper, valueMapper);

            }).collect(toList());
        }
    }
}
