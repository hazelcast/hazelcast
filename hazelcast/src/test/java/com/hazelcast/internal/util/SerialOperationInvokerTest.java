/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.SimpleMemberImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterTopologyChangedException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.hazelcast.instance.impl.TestUtil.getNode;
import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;
import static com.hazelcast.spi.properties.ClusterProperty.INVOCATION_RETRY_PAUSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SerialOperationInvokerTest extends HazelcastTestSupport {
    @Rule
    public TestName testNameRule = new TestName();
    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    private Config config;
    private HazelcastInstance instance1;
    private HazelcastInstance instance2;
    private HazelcastInstance instance3;

    @Before
    public void setup() {
        config = smallInstanceConfig();
        config.getJetConfig().setEnabled(false);
        config.setProperty(INVOCATION_RETRY_PAUSE.getName(), "10");
        instance1 = factory.newHazelcastInstance(config);
        instance2 = factory.newHazelcastInstance(config);
        instance3 = factory.newHazelcastInstance(config);
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        InvokedMemberRecordingOperation.TEST_NAME_TO_INVOKED_MEMBER_UUIDS.clear();
    }

    @Test
    public void testInvoke() {
        String testName = testNameRule.getMethodName();

        Node node = getNode(instance1);
        InternalCompletableFuture<Object> clusterSerial = invokeOnStableClusterSerial(
                node.getNodeEngine(),
                () -> new InvokedMemberRecordingOperation(testName),
                0
        );

        clusterSerial.join();

        Collection<UUID> expectedUuids = getMembersUuids(node);
        assertExpectedUuids(testName, expectedUuids);
    }

    @Test
    public void testInvoke_withSingleMember() {
        String testName = testNameRule.getMethodName();

        instance2.shutdown();
        instance3.shutdown();

        assertClusterSizeEventually(1, instance1);

        Node node = getNode(instance1);
        InternalCompletableFuture<Object> clusterSerial = invokeOnStableClusterSerial(
                node.getNodeEngine(),
                () -> new InvokedMemberRecordingOperation(testName),
                0
        );

        clusterSerial.join();

        Collection<UUID> expectedUuids = getMembersUuids(node);
        assertExpectedUuids(testName, expectedUuids);
    }

    @Test
    public void testInvoke_withDelayedOperations() {
        String testName = testNameRule.getMethodName();

        Node node = getNode(instance1);
        InternalCompletableFuture<Object> clusterSerial = invokeOnStableClusterSerial(
                node.getNodeEngine(),
                () -> new InvokedMemberRecordingDelayedOperation(testName),
                0
        );

        clusterSerial.join();

        Collection<UUID> expectedUuids = getMembersUuids(node);
        assertExpectedUuids(testName, expectedUuids);
    }

    @Test
    public void testInvoke_IsRestarted_whenAddedMember() {
        String testName = testNameRule.getMethodName();

        Node node = getNode(instance1);
        InternalCompletableFuture<Object> clusterSerial = invokeOnStableClusterSerial(
                node.getNodeEngine(),
                () -> new AwaitingAddMemberOperation(testName, 3, 4),
                2
        );

        // Wait for this operation's INITIAL_OPS_LATCH to count down (ensuring that our initial
        // offering has been accepted), otherwise we can encounter a race condition where the
        // invocation is restarted before the initial member UUIDs have been fully collected
        assertOpenEventually(AwaitingAddMemberOperation.INITIAL_OPS_LATCH, 5);

        Collection<UUID> expectedUuids = new ArrayList<>(getMembersUuids(node));

        HazelcastInstance instance4 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(4, instance1, instance2, instance3, instance4);

        // Count down on this operation's CLUSTER_SIZE_LATCH to allow progression now that
        // the cluster size change has been successfully completed
        AwaitingAddMemberOperation.CLUSTER_SIZE_LATCH.countDown();

        clusterSerial.join();

        expectedUuids.addAll(getMembersUuids(node));
        assertExpectedUuids(testName, expectedUuids);
    }

    @Test
    public void testInvoke_IsRestarted_whenRemovedMember() {
        String testName = testNameRule.getMethodName();

        Node node = getNode(instance1);
        InternalCompletableFuture<Object> clusterSerial = invokeOnStableClusterSerial(
                node.getNodeEngine(),
                () -> new AwaitingRemoveMemberOperation(testName, 3, 2),
                2
        );

        // Wait for this operation's INITIAL_OPS_LATCH to count down (ensuring that our initial
        // offering has been accepted), otherwise we can encounter a race condition where the
        // invocation is restarted before the initial member UUIDs have been fully collected
        assertOpenEventually(AwaitingRemoveMemberOperation.INITIAL_OPS_LATCH, 5);

        instance3.shutdown();
        assertClusterSizeEventually(2, instance1, instance2);

        // Count down on this operation's CLUSTER_SIZE_LATCH to allow progression now that
        // the cluster size change has been successfully completed
        AwaitingRemoveMemberOperation.CLUSTER_SIZE_LATCH.countDown();

        Collection<UUID> expectedUuids = new ArrayList<>(getMembersUuids(node));
        clusterSerial.join();

        expectedUuids.addAll(getMembersUuids(node));
        assertExpectedUuids(testName, expectedUuids);
    }

    @Test
    public void testInvoke_withThrowingTerminalException() {
        String testName = testNameRule.getMethodName();

        Node node = getNode(instance2);
        assertThatThrownBy(() -> invokeOnStableClusterSerial(
                node.getNodeEngine(),
                () -> new ExceptionThrowingOperation(testName, new RuntimeException("expected")),
                0
        ).join()).isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("expected");
    }

    @Test
    public void testInvoke_withThrowingCompletionExceptionWithNullCause() {
        String testName = testNameRule.getMethodName();

        Node node = getNode(instance2);
        assertThatThrownBy(() -> invokeOnStableClusterSerial(
                node.getNodeEngine(),
                () -> new ExceptionThrowingOperation(testName, new CompletionException(null)),
                0
        ).join()).isInstanceOf(CompletionException.class)
                .hasCause(null);
    }

    @Test
    public void testInvoke_withThrowingIgnorableTargetNotMemberException() {
        assertExceptionIgnored(new TargetNotMemberException("expected"));
    }

    @Test
    public void testInvoke_withThrowingIgnorableMemberLeftException() {
        SimpleMemberImpl member = new SimpleMemberImpl(
                MemberVersion.UNKNOWN,
                UUID.randomUUID(),
                new InetSocketAddress("127.0.0.1", 55555)
        );
        assertExceptionIgnored(new MemberLeftException(member));
    }

    @Test
    public void testInvoke_withThrowingIgnorableHazelcastInstanceNotActiveException() {
        assertExceptionIgnored(new HazelcastInstanceNotActiveException("expected"));
    }

    private void assertExceptionIgnored(Exception exception) {
        String testName = testNameRule.getMethodName();

        Node node = getNode(instance3);
        InternalCompletableFuture<Object> clusterSerial = invokeOnStableClusterSerial(
                node.getNodeEngine(),
                () -> new ExceptionThrowingOperation(testName, exception),
                2
        );

        clusterSerial.join();

        Collection<UUID> expectedUuids = getMembersUuids(node);
        assertExpectedUuids(testName, expectedUuids);
    }

    @Test
    public void testInvoke_withThrowingTopologyChangeException() {
        String testName = testNameRule.getMethodName();

        Node node = getNode(instance3);
        assertThatThrownBy(() -> invokeOnStableClusterSerial(
                node.getNodeEngine(),
                () -> new ExceptionThrowingOperation(testName, new ClusterTopologyChangedException("expected")),
                20
        ).join()).isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(HazelcastException.class)
                .hasStackTraceContaining("Cluster topology was not stable for 20 retries");
    }

    private List<UUID> getMembersUuids(Node node) {
        return node.getNodeEngine().getClusterService().getMembers().stream()
                .map(Member::getUuid)
                .collect(Collectors.toList());
    }

    private void assertExpectedUuids(String testName, Collection<UUID> expectedUuids) {
        assertThat(InvokedMemberRecordingOperation.TEST_NAME_TO_INVOKED_MEMBER_UUIDS.get(testName))
                .containsExactlyElementsOf(expectedUuids);
    }

    private static class ExceptionThrowingOperation extends InvokedMemberRecordingOperation {
        private Exception exception;

        ExceptionThrowingOperation() {
        }

        ExceptionThrowingOperation(String testName, Exception exception) {
            super(testName);
            this.exception = exception;
        }

        @Override
        public void run() throws Exception {
            super.run();
            throw exception;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeObject(exception);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            exception = in.readObject();
        }

        @Override
        public ExceptionAction onInvocationException(Throwable throwable) {
            return ExceptionAction.THROW_EXCEPTION;
        }
    }

    private static class InvokedMemberRecordingOperation extends Operation {
        protected static final Map<String, Collection<UUID>> TEST_NAME_TO_INVOKED_MEMBER_UUIDS = new ConcurrentHashMap<>();
        protected String testName;

        InvokedMemberRecordingOperation() {
        }

        InvokedMemberRecordingOperation(String testName) {
            this.testName = testName;
        }

        @Override
        public void run() throws Exception {
            TEST_NAME_TO_INVOKED_MEMBER_UUIDS
                    .computeIfAbsent(testName, k -> new CopyOnWriteArrayList<>())
                    .add(getNodeEngine().getLocalMember().getUuid());
            super.run();
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeString(testName);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            testName = in.readString();
        }
    }

    private static class AwaitingAddMemberOperation extends InvokedMemberRecordingOperation {

        private static final AtomicInteger COUNT = new AtomicInteger(0);
        private static final CountDownLatch INITIAL_OPS_LATCH = new CountDownLatch(3);
        private static final CountDownLatch CLUSTER_SIZE_LATCH = new CountDownLatch(1);
        private int membersCount;
        private int expectedMemberCount;

        AwaitingAddMemberOperation() {
        }

        AwaitingAddMemberOperation(String testName, int membersCount, int expectedMemberCount) {
            super(testName);
            this.membersCount = membersCount;
            this.expectedMemberCount = expectedMemberCount;
        }

        @Override
        public void run() throws Exception {
            // We use this latch to ensure that all initial operations have been
            // completed before we alter the cluster topology (which results in
            // this operation being restarted)
            INITIAL_OPS_LATCH.countDown();

            if (COUNT.getAndIncrement() == (membersCount - 1)) {
                // At this point we need the operation to wait until the topology
                // is changed, so that it does not finish early (which would result
                // in the operation not being restarted)
                CLUSTER_SIZE_LATCH.await();
            }
            super.run();
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeInt(membersCount);
            out.writeInt(expectedMemberCount);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            membersCount = in.readInt();
            expectedMemberCount = in.readInt();
        }
    }

    private static class AwaitingRemoveMemberOperation extends InvokedMemberRecordingOperation {

        private static final AtomicInteger COUNT = new AtomicInteger(0);
        private static final CountDownLatch INITIAL_OPS_LATCH = new CountDownLatch(3);
        private static final CountDownLatch CLUSTER_SIZE_LATCH = new CountDownLatch(1);
        private int membersCount;
        private int expectedMemberCount;

        AwaitingRemoveMemberOperation() {
        }

        AwaitingRemoveMemberOperation(String testName, int membersCount, int expectedMemberCount) {
            super(testName);
            this.membersCount = membersCount;
            this.expectedMemberCount = expectedMemberCount;
        }

        @Override
        public void run() throws Exception {
            // We use this latch to ensure that all initial operations have been
            // completed before we alter the cluster topology (which results in
            // this operation being restarted)
            INITIAL_OPS_LATCH.countDown();

            if (COUNT.getAndIncrement() == (membersCount - 1)) {
                // At this point we need the operation to wait until the topology
                // is changed, so that it does not finish early (which would result
                // in the operation not being restarted)
                CLUSTER_SIZE_LATCH.await();
            }
            super.run();
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeInt(membersCount);
            out.writeInt(expectedMemberCount);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            membersCount = in.readInt();
            expectedMemberCount = in.readInt();
        }
    }

    private static class InvokedMemberRecordingDelayedOperation extends InvokedMemberRecordingOperation {
        private static final AtomicInteger delaySec = new AtomicInteger(3);

        InvokedMemberRecordingDelayedOperation() {
        }

        InvokedMemberRecordingDelayedOperation(String testName) {
            super(testName);
        }

        @Override
        public void run() throws Exception {
            sleepSeconds(delaySec.getAndDecrement());
            super.run();
        }
    }
}
