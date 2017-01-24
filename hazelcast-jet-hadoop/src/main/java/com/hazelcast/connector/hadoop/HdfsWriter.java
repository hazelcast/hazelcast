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

package com.hazelcast.connector.hadoop;


import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TextOutputFormat;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.unchecked;
import static java.lang.String.valueOf;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.mapreduce.TaskType.JOB_SETUP;

/**
 * HDFS writer for Jet, consumes Map.Entry objects and writes them to the output file in HDFS.
 */
public final class HdfsWriter extends AbstractProcessor {

    private static final ILogger LOGGER = Logger.getLogger(HdfsWriter.class);

    private final RecordWriter recordWriter;
    private final TaskAttemptContextImpl taskAttemptContext;
    private final OutputCommitter outputCommitter;

    private HdfsWriter(RecordWriter recordWriter, TaskAttemptContextImpl taskAttemptContext,
                       OutputCommitter outputCommitter) {
        this.recordWriter = recordWriter;
        this.taskAttemptContext = taskAttemptContext;
        this.outputCommitter = outputCommitter;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        Map.Entry entry = (Map.Entry) item;
        return uncheckCall(() -> {
            recordWriter.write(entry.getKey(), entry.getValue());
            return true;
        });
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
     * Creates supplier for writing HDFS files.
     *
     * @param path output path for writing files
     * @return {@link ProcessorMetaSupplier} supplier
     */
    public static ProcessorMetaSupplier supplier(String path) {
        return new MetaSupplier(path);
    }

    private static class MetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final String path;
        private transient Address address;

        MetaSupplier(String path) {
            this.path = path;
        }

        @Override
        public void init(@Nonnull Context context) {
            address = context.jetInstance().getCluster().getLocalMember().getAddress();
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier(address.equals(this.address), path);
        }
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final boolean commitJob;
        private final String path;
        private transient Context context;
        private transient OutputCommitter outputCommitter;
        private transient JobConf conf;
        private transient JobID jobId;
        private transient JobContextImpl jobContext;

        Supplier(boolean commitJob, String path) {
            this.commitJob = commitJob;
            this.path = path;
        }

        @Override
        public void init(@Nonnull Context context) {
            this.context = context;
            conf = new JobConf();
            conf.setOutputFormat(TextOutputFormat.class);
            conf.setOutputCommitter(FileOutputCommitter.class);
            TextOutputFormat.setOutputPath(conf, new Path(path));
            outputCommitter = conf.getOutputCommitter();
            jobId = new JobID();
            jobContext = new JobContextImpl(conf, jobId);

        }

        @Override
        public void complete(Throwable error) {
            if (commitJob) {
                try {
                    outputCommitter.commitJob(jobContext);
                } catch (IOException e) {
                    throw unchecked(e);
                }
            }
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return IntStream.range(0, count)
                    .mapToObj(i -> {
                        if (i == 0) {
                            uncheckCall(() -> {
                                outputCommitter.setupJob(jobContext);
                                return null;
                            });
                        }
                        String uuid = context.jetInstance().getCluster().getLocalMember().getUuid();
                        TaskAttemptID taskAttemptID = new TaskAttemptID("jet-node-" + uuid, jobId.getId(),
                                JOB_SETUP, i, 0);
                        conf.set("mapred.task.id", taskAttemptID.toString());
                        conf.setInt("mapred.task.partition", i);

                        TaskAttemptContextImpl taskAttemptContext = new TaskAttemptContextImpl(conf, taskAttemptID);
                        RecordWriter recordWriter = uncheckCall(() -> conf.getOutputFormat().getRecordWriter(null,
                                conf, uuid + '-' + valueOf(i), Reporter.NULL));
                        return new HdfsWriter(recordWriter, taskAttemptContext, outputCommitter);

                    })
                    .collect(toList());
        }
    }
}
