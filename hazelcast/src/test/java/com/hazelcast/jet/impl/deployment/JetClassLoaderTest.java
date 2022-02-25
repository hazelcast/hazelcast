/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
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
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.processor.NoopP;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetClassLoaderTest extends JetTestSupport {

    @Test
    public void when_jobCompleted_then_classLoaderShutDown() {
        DAG dag = new DAG();
        dag.newVertex("v", LeakClassLoaderP::new).localParallelism(1);

        Config config = smallInstanceWithResourceUploadConfig();
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

        assertClassLoaders(TargetP.classLoaders);

        assertClassLoaderForAllMethodsChecked(Processor.class, TargetP.classLoaders);
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

    @Test
    public void when_processorSupplierCalled_then_contextClassLoaderSet() {
        DAG dag = new DAG();
        dag.newVertex("v", new LeakClassLoaderPS()).localParallelism(1);

        Config config = smallInstanceWithResourceUploadConfig();
        HazelcastInstance instance = createHazelcastInstance(config);

        // When
        instance.getJet().newJob(dag).join();

        assertClassLoaders(LeakClassLoaderPS.classLoaders);
        assertThat(LeakClassLoaderPS.classLoaders)
                .containsKeys("init", "get", "close");
    }

    private static class LeakClassLoaderPS implements ProcessorSupplier {

        private static Map<String, List<ClassLoader>> classLoaders = new HashMap<>();

        @Override
        public void init(@Nonnull Context context) throws Exception {
            putClassLoader("init");
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            putClassLoader("get");
            return Collections.singleton(new NoopP());
        }

        @Override
        public void close(@Nullable Throwable error) throws Exception {
            putClassLoader("close");
        }

        private void putClassLoader(String methodName) {
            List<ClassLoader> cls = classLoaders.computeIfAbsent(methodName, (key) -> new ArrayList<>());
            cls.add(Thread.currentThread().getContextClassLoader());
        }
    }

    @Test
    public void when_processorMetaSupplierCalled_then_contextClassLoaderSet() {
        DAG dag = new DAG();
        dag.newVertex("v", new LeakClassLoaderPMS()).localParallelism(1);

        Config config = smallInstanceWithResourceUploadConfig();
        HazelcastInstance instance = createHazelcastInstance(config);

        // When
        instance.getJet().newJob(dag).join();

        assertClassLoaders(LeakClassLoaderPMS.classLoaders);
        assertThat(LeakClassLoaderPMS.classLoaders)
                .containsKeys("init", "get", "close");
    }

    private static class LeakClassLoaderPMS implements ProcessorMetaSupplier {

        private static Map<String, List<ClassLoader>> classLoaders = new HashMap<>();

        @Override
        public void init(@Nonnull Context context) throws Exception {
            putClassLoader("init");
        }

        @Nonnull
        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            putClassLoader("get");
            return address -> ProcessorSupplier.of(NoopP::new);
        }

        @Override
        public void close(@Nullable Throwable error) throws Exception {
            putClassLoader("close");
        }

        private void putClassLoader(String methodName) {
            List<ClassLoader> cls = classLoaders.computeIfAbsent(methodName, (key) -> new ArrayList<>());
            cls.add(Thread.currentThread().getContextClassLoader());
        }
    }

    private void assertClassLoaderForAllMethodsChecked(
            Class<?> clazz,
            Map<String, List<ClassLoader>> classLoaders) {

        // Future-proof against Processor API additions
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }

            String name = method.getName();
            assertThat(classLoaders)
                    .describedAs("method " + name + " not called")
                    .containsKey(name);
        }
    }

    private void assertClassLoaders(Map<String, List<ClassLoader>> classLoaders) {
        for (Map.Entry<String, List<ClassLoader>> entry : classLoaders.entrySet()) {
            List<ClassLoader> cls = entry.getValue();
            for (ClassLoader cl : cls) {
                assertThat(cl)
                        .describedAs("expecting JetClassLoader for method " + entry.getKey())
                        .isInstanceOf(JetClassLoader.class);
            }
        }
    }
}
