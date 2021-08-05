/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetClassLoaderTest extends JetTestSupport {

    @Test
    public void when_jobCompleted_then_classLoaderShutDown() {
        DAG dag = new DAG();
        dag.newVertex("v", LeakClassLoaderP::new).localParallelism(1);

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance instance = createHazelcastInstance(config);

        // When
        instance.getJet().newJob(dag).join();

        // Then
        assertTrue("The classloader should have been shutdown after job completion",
                LeakClassLoaderP.classLoader.isShutdown()
        );
    }

    private static class LeakClassLoaderP extends AbstractProcessor {

        private static volatile JetClassLoader classLoader;

        @Override
        public boolean complete() {
            classLoader = (JetClassLoader) Thread.currentThread().getContextClassLoader();
            return true;
        }

    }

    @Test
    public void when_processorCalled_then_contextClassLoaderSet() {
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", SourceP::new).localParallelism(1);
        Vertex v2 = dag.newVertex("v2", TargetP::new).localParallelism(1);
        dag.edge(Edge.between(v1, v2));

        HazelcastInstance instance = createHazelcastInstance(smallInstanceWithResourceUploadConfig());

        JobConfig jobConfig = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE)
                .setSnapshotIntervalMillis(1);

        Job job = instance.getJet().newJob(dag, jobConfig);

        assertJobStatusEventually(job, JobStatus.RUNNING);
        job.suspend();
        assertJobStatusEventually(job, JobStatus.SUSPENDED);
        job.resume();
        job.join();

        for (Map.Entry<String, List<ClassLoader>> entry : TargetP.classLoaders.entrySet()) {
            List<ClassLoader> cls = entry.getValue();
            for (ClassLoader cl : cls) {
                assertThat(cl)
                        .describedAs("expecting JetClassLoader for method " + entry.getKey())
                        .isInstanceOf(JetClassLoader.class);
            }
        }

        // Future-proof against Processor API additions
        Method[] methods = Processor.class.getMethods();
        for (Method method : methods) {
            String name = method.getName();
            assertThat(TargetP.classLoaders)
                    .describedAs("method " + name + " not called")
                    .containsKey(name);
        }
    }

    /**
     * Special source that emits one event and one watermark and completing only after a restore from a snapshot
     * As a consequence on a downstream processor (see {@link TargetP}) all Processor methods will be called.
     */
    private static class SourceP extends AbstractProcessor {

        private volatile boolean emitted = false;
        private volatile boolean emittedWm = false;
        private volatile boolean restored = false;

        @Override
        public boolean complete() {
            if (!emitted) {
                emitted = tryEmit(1);
                return false;
            } else if (!emittedWm) {
                emittedWm = tryEmit(new Watermark(System.currentTimeMillis()));
                return false;
            } else {
                // This source will complete only after restoring from snapshot (suspend -> resume)
                return restored;
            }
        }

        @Override
        public boolean finishSnapshotRestore() {
            restored = true;
            return super.finishSnapshotRestore();
        }

    }

    private static class TargetP extends AbstractProcessor {

        private volatile boolean received = false;
        private volatile boolean restored = false;

        private static Map<String, List<ClassLoader>> classLoaders = new HashMap<>();

        private void putClassLoader(String methodName) {
            List<ClassLoader> cls = classLoaders.computeIfAbsent(methodName, (key) -> new ArrayList<>());
            cls.add(Thread.currentThread().getContextClassLoader());
        }

        @Override
        public boolean isCooperative() {
            putClassLoader("isCooperative");
            return super.isCooperative();
        }

        @Override
        protected void init(@Nonnull Context context) throws Exception {
            putClassLoader("init");
            super.init(context);
        }

        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            putClassLoader("process");
            super.process(ordinal, inbox);
        }

        @Override
        public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
            putClassLoader("tryProcessWatermark");
            return super.tryProcessWatermark(watermark);
        }

        @Override
        public boolean tryProcess() {
            putClassLoader("tryProcess");
            return super.tryProcess();
        }

        @Override
        public boolean completeEdge(int ordinal) {
            putClassLoader("completeEdge");
            return super.completeEdge(ordinal);
        }

        @Override
        public boolean complete() {
            putClassLoader("complete");
            return restored && received;
        }

        @Override
        public boolean saveToSnapshot() {
            getOutbox().offerToSnapshot(1, 1);
            putClassLoader("saveToSnapshot");
            return super.saveToSnapshot();
        }

        @Override
        public boolean snapshotCommitPrepare() {
            putClassLoader("snapshotCommitPrepare");
            return super.snapshotCommitPrepare();
        }

        @Override
        public boolean snapshotCommitFinish(boolean success) {
            putClassLoader("snapshotCommitFinish");
            return super.snapshotCommitFinish(success);
        }

        @Override
        protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            putClassLoader("restoreFromSnapshot");
        }

        @Override
        public boolean finishSnapshotRestore() {
            restored = true;
            putClassLoader("finishSnapshotRestore");
            return super.finishSnapshotRestore();
        }

        @Override
        public void close() throws Exception {
            putClassLoader("close");
            super.close();
        }

        @Override
        public boolean closeIsCooperative() {
            putClassLoader("closeIsCooperative");
            return super.closeIsCooperative();
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
            received = true;
            return true;
        }
    }

}
