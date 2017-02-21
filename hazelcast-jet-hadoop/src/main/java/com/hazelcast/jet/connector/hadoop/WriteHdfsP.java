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
import java.util.function.Function;

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

    private WriteHdfsP(RecordWriter recordWriter, TaskAttemptContextImpl taskAttemptContext,
                       OutputCommitter outputCommitter) {
        this.recordWriter = recordWriter;
        this.taskAttemptContext = taskAttemptContext;
        this.outputCommitter = outputCommitter;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        Map.Entry entry = (Map.Entry) item;
        recordWriter.write(entry.getKey(), entry.getValue());
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
        return new MetaSupplier(jobConf);
    }

    private static class MetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final JobConfiguration configuration;
        private transient Address address;

        MetaSupplier(JobConf jobConf) {
            this.configuration = new JobConfiguration(jobConf);
        }

        @Override
        public void init(@Nonnull Context context) {
            address = context.jetInstance().getCluster().getLocalMember().getAddress();
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier(address.equals(this.address), configuration);
        }
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private boolean commitJob;
        private JobConfiguration configuration;

        private transient Context context;
        private transient OutputCommitter outputCommitter;
        private transient JobID jobId;
        private transient JobContextImpl jobContext;

        Supplier(boolean commitJob, JobConfiguration configuration) {
            this.commitJob = commitJob;
            this.configuration = configuration;
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
                return new WriteHdfsP(recordWriter, taskAttemptContext, outputCommitter);

            }).collect(toList());
        }
    }
}
