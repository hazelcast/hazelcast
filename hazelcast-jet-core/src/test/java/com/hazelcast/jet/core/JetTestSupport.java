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

package com.hazelcast.jet.core;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.ICacheJet;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.util.Util.RunnableExc;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.idToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class JetTestSupport extends HazelcastTestSupport {

    protected ILogger logger = Logger.getLogger(getClass());
    private JetTestInstanceFactory instanceFactory;

    @After
    public void shutdownFactory() {
        if (instanceFactory != null) {
            instanceFactory.terminateAll();
        }
    }

    protected JetInstance createJetClient() {
        return instanceFactory.newClient();
    }

    protected JetInstance createJetClient(ClientConfig config) {
        return instanceFactory.newClient(config);
    }

    protected JetInstance createJetMember() {
        return this.createJetMember(new JetConfig());
    }

    protected JetInstance createJetMember(JetConfig config) {
        if (instanceFactory == null) {
            instanceFactory = new JetTestInstanceFactory();
        }
        return instanceFactory.newMember(config);
    }

    protected JetInstance[] createJetMembers(int nodeCount) {
        return createJetMembers(new JetConfig(), nodeCount);
    }

    protected JetInstance[] createJetMembers(JetConfig config, int nodeCount) {
        if (instanceFactory == null) {
            instanceFactory = new JetTestInstanceFactory();
        }
        return instanceFactory.newMembers(config, nodeCount);
    }

    protected JetInstance createJetMember(JetConfig config, Address[] blockedAddress) {
        if (instanceFactory == null) {
            instanceFactory = new JetTestInstanceFactory();
        }
        return instanceFactory.newMember(config, blockedAddress);
    }

    protected static <K, V> IMapJet<K, V> getMap(JetInstance instance) {
        return instance.getMap(randomName());
    }

    protected static <K, V> ICacheJet<K, V> getCache(JetInstance instance) {
        return instance.getCacheManager().getCache(randomName());
    }

    protected static void fillMapWithInts(IMap<Integer, Integer> map, int count) {
        Map<Integer, Integer> vals = IntStream.range(0, count).boxed().collect(Collectors.toMap(m -> m, m -> m));
        map.putAll(vals);
    }

    protected static void fillListWithInts(IList<Integer> list, int count) {
        for (int i = 0; i < count; i++) {
            list.add(i);
        }
    }

    protected static <E> IListJet<E> getList(JetInstance instance) {
        return instance.getList(randomName());
    }

    protected static void appendToFile(File file, String... lines) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            for (String payload : lines) {
                writer.write(payload + '\n');
            }
        }
    }

    protected static File createTempDirectory() throws IOException {
        Path directory = Files.createTempDirectory("jet-test-temp");
        File file = directory.toFile();
        file.deleteOnExit();
        return file;
    }

    public static void assertJobStatusEventually(Job job, JobStatus expected) {
        assertJobStatusEventually(job, expected, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertJobStatusEventually(Job job, JobStatus expected, int timeoutSeconds) {
        assertNotNull(job);
        assertTrueEventually(() ->
                assertEquals("jobId=" + idToString(job.getId()), expected, job.getStatus()), timeoutSeconds);
    }

    public static void assertTrueEventually(RunnableExc runnable) {
        HazelcastTestSupport.assertTrueEventually(assertTask(runnable));
    }

    public static void assertTrueEventually(RunnableExc runnable, long timeoutSeconds) {
        HazelcastTestSupport.assertTrueEventually(assertTask(runnable), timeoutSeconds);
    }

    public static void assertTrueAllTheTime(RunnableExc runnable, long durationSeconds) {
        HazelcastTestSupport.assertTrueAllTheTime(assertTask(runnable), durationSeconds);
    }

    public static void assertTrueFiveSeconds(RunnableExc runnable) {
        HazelcastTestSupport.assertTrueFiveSeconds(assertTask(runnable));
    }

    public static JetService getJetService(JetInstance jetInstance) {
        return getNodeEngineImpl(jetInstance).getService(JetService.SERVICE_NAME);
    }

    public static HazelcastInstance hz(JetInstance instance) {
        return instance.getHazelcastInstance();
    }

    public static Address getAddress(JetInstance instance) {
        return getAddress(hz(instance));
    }

    public static Node getNode(JetInstance instance) {
        return getNode(hz(instance));
    }

    public static NodeEngineImpl getNodeEngineImpl(JetInstance instance) {
        return getNodeEngineImpl(hz(instance));
    }

    private static <T extends Exception> AssertTask assertTask(RunnableExc<T> runnable) {
        return new AssertTask() {
            @Override
            public void run() throws T {
                runnable.run();
            }
        };
    }

    public Address nextAddress() {
        return instanceFactory.nextAddress();
    }

    protected void terminateInstance(JetInstance instance) {
        instanceFactory.terminate(instance);
    }

    public Future spawnSafe(RunnableExc r) {
        return spawn(() -> {
            try {
                r.run();
            } catch (Exception e) {
                logger.warning("Spawned Runnable failed", e);
            }
        });
    }

    public static Watermark wm(long timestamp) {
        return new Watermark(timestamp);
    }

    public void waitForFirstSnapshot(JobRepository jr, long jobId, int timeoutSeconds, boolean allowEmptySnapshot) {
        long[] snapshotId = {-1};
        assertTrueEventually(() -> {
            JobExecutionRecord record = jr.getJobExecutionRecord(jobId);
            assertNotNull("null JobExecutionRecord", record);
            assertTrue("No snapshot produced",
                    record.dataMapIndex() >= 0 && record.snapshotId() >= 0);
            assertTrue("stats are 0", allowEmptySnapshot || record.snapshotStats().numBytes() > 0);
            snapshotId[0] = record.snapshotId();
        }, timeoutSeconds);
        logger.info("First snapshot found (id=" + snapshotId[0] + ")");
    }

    public void waitForNextSnapshot(JobRepository jr, long jobId, int timeoutSeconds) {
        long originalSnapshotId = jr.getJobExecutionRecord(jobId).snapshotId();
        // wait until there is at least one more snapshot
        long[] snapshotId = {-1};
        assertTrueEventually(() -> {
            JobExecutionRecord record = jr.getJobExecutionRecord(jobId);
            assertNotNull("jobExecutionRecord is null", record);
            snapshotId[0] = record.snapshotId();
            assertTrue("No more snapshots produced after restart in " + timeoutSeconds + " seconds",
                    snapshotId[0] > originalSnapshotId);
            assertTrue("stats are 0", record.snapshotStats().numBytes() > 0);
        }, timeoutSeconds);
        logger.info("Next snapshot found (id=" + snapshotId[0] + ", previous id=" + originalSnapshotId + ")");
    }
}
