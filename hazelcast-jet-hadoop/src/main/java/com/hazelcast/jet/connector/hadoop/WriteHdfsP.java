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
import static com.hazelcast.jet.connector.hadoop.SerializableJobConf.asSerializable;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.lang.String.valueOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.hadoop.mapreduce.TaskType.JOB_SETUP;

/**
 * A processor which consumes records and writes them to HDFS.
 *
 * The records are written according to the given {@code OutputFormat}. The
 * processor expects incoming items to be of type {@code Map.Entry<K,V>} and also
 * accepts optional mappers to transform the key and the value to another type, such as
 * a {@code Writable} that is expected by some of the output formats.
 *
 * Each processor instance creates a single file in the output path identified by
 * the member ID and the processor ID. Unlike in MapReduce,
 * the output files are not sorted by key.
 *
 * @param <K>         the key type of the records
 * @param <KM>        the type of the key after mapping
 * @param <V>         the value type of the records
 * @param <VM>        the type of the value after mapping
 */
public final class WriteHdfsP<K, KM, V, VM> extends AbstractProcessor {

    private final RecordWriter<KM, VM> recordWriter;
    private final TaskAttemptContextImpl taskAttemptContext;
    private final OutputCommitter outputCommitter;
    private final Function<K, KM> keyMapper;
    private final Function<V, VM> valueMapper;

    private WriteHdfsP(RecordWriter<KM, VM> recordWriter, TaskAttemptContextImpl taskAttemptContext,
                       OutputCommitter outputCommitter, Function<K, KM> keyMapper, Function<V, VM> valueMapper) {
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

    /**
     * Returns a meta-supplier of processors that writes to HDFS. The processors expect
     * items of type {@code Map.Entry<K,V>}. The key and the value must be of a type that
     * is writable to disk by the given {@code OutputFormat} in the configuration.
     *
     * The supplied {@code JobConf} must specify an {@code OutputFormat} with a path.
     *
     * @param jobConf     {@code JobConf} used for output format configuration
     *
     * @param <K>         the key type of the records
     * @param <V>         the value type of the records
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier writeHdfs(@Nonnull JobConf jobConf) {
        return WriteHdfsP.writeHdfs(jobConf, identity(), identity());
    }

    /**
     * Returns a meta-supplier of processors that writes to HDFS. The processors expect
     * items of type {@code Map.Entry<K,V>} and take optional mappers for converting
     * the key and the value to types required by the output format. For example, the mappers
     * can be used to map the keys and the values to their {@code Writable} equivalents.
     *
     * The supplied {@code JobConf} supplied must specify an {@code OutputFormat} with a path.
     *
     * @param jobConf     {@code JobConf} used for output format configuration
     * @param keyMapper   mapper which can be used to map a key to another key
     * @param valueMapper mapper which can be used to map a value to another value
     *
     * @param <K>         the key type of the records
     * @param <KM>        the type of the key after mapping
     * @param <V>         the value type of the records
     * @param <VM>        the type of the value after mapping
     */
    @Nonnull
    public static <K, KM, V, VM> ProcessorMetaSupplier writeHdfs(@Nonnull JobConf jobConf,
                                                                 @Nonnull Function<K, KM> keyMapper,
                                                                 @Nonnull Function<V, VM> valueMapper) {
        return new MetaSupplier<>(asSerializable(jobConf), keyMapper, valueMapper);
    }


    private static class MetaSupplier<K, KM, V, VM> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableJobConf jobConf;
        private final Function<K, KM> keyMapper;
        private final Function<V, VM> valueMapper;

        private transient Address address;

        MetaSupplier(SerializableJobConf jobConf, Function<K, KM> keyMapper, Function<V, VM> valueMapper) {
            this.jobConf = jobConf;
            this.keyMapper = keyMapper;
            this.valueMapper = valueMapper;
        }

        @Override
        public void init(@Nonnull Context context) {
            address = context.jetInstance().getCluster().getLocalMember().getAddress();
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier<>(address.equals(this.address), jobConf, keyMapper, valueMapper);
        }
    }

    private static class Supplier<K, KM, V, VM> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final boolean commitJob;
        private final SerializableJobConf jobConf;
        private final Function<K, KM> keyMapper;
        private final Function<V, VM> valueMapper;

        private transient Context context;
        private transient OutputCommitter outputCommitter;
        private transient JobContextImpl jobContext;

        Supplier(boolean commitJob, SerializableJobConf jobConf, Function<K, KM> keyMapper, Function<V, VM> valueMapper) {
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
