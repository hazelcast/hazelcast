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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.hazelcast.instance.impl.TestUtil.getNode;
import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterParallel;
import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterParallelExcludeLocal;
import static com.hazelcast.spi.properties.ClusterProperty.INVOCATION_RETRY_PAUSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class ParallelOperationInvokerTest extends HazelcastTestSupport {
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
        Node node = getNode(instance1);
        Collection<UUID> uuids = invokeOnStableClusterParallel(
                node.getNodeEngine(),
                NoOpOperation::new,
                0
        ).join();

        Collection<UUID> expectedUuids = getUuidsOfInstances(instance1, instance2, instance3);
        assertThat(uuids)
                .containsExactlyInAnyOrderElementsOf(expectedUuids);
    }

    @Test
    public void testInvokeWithFilter() {
        String testName = testNameRule.getMethodName();

        Node node = getNode(instance1);
        Collection<UUID> uuids = invokeOnStableClusterParallel(
                node.getNodeEngine(),
                () -> new InvokedMemberRecordingOperation(testName),
                0,
                member -> member.getUuid().equals(instance2.getLocalEndpoint().getUuid())
        ).join();

        Collection<UUID> expectedUuids = getUuidsOfInstances(instance1, instance2, instance3);
        assertThat(uuids)
                .containsExactlyInAnyOrderElementsOf(expectedUuids);

        expectedUuids = getUuidsOfInstances(instance2);
        assertThat(InvokedMemberRecordingOperation.TEST_NAME_TO_INVOKED_MEMBER_UUIDS.get(testName))
                .containsExactlyInAnyOrderElementsOf(expectedUuids);
    }

    @Test
    public void testInvokeWithExcludeLocalFilter() {
        String testName = testNameRule.getMethodName();

        Node node = getNode(instance1);
        Collection<UUID> uuids = invokeOnStableClusterParallelExcludeLocal(
                node.getNodeEngine(),
                () -> new InvokedMemberRecordingOperation(testName),
                0
        ).join();

        Collection<UUID> expectedUuids = getUuidsOfInstances(instance1, instance2, instance3);
        assertThat(uuids)
                .containsExactlyInAnyOrderElementsOf(expectedUuids);

        expectedUuids = getUuidsOfInstances(instance2, instance3);
        assertThat(InvokedMemberRecordingOperation.TEST_NAME_TO_INVOKED_MEMBER_UUIDS.get(testName))
                .containsExactlyInAnyOrderElementsOf(expectedUuids);
    }

    @Test
    public void testInvoke_withSingleMember() {
        instance2.shutdown();
        instance3.shutdown();

        assertClusterSizeEventually(1, instance1);

        Node node = getNode(instance1);
        Collection<UUID> uuids = invokeOnStableClusterParallel(
                node.getNodeEngine(),
                NoOpOperation::new,
                0
        ).join();

        Collection<UUID> expectedUuids = getUuidsOfInstances(instance1);
        assertThat(uuids)
                .containsExactlyInAnyOrderElementsOf(expectedUuids);
    }

    @Test
    public void testInvoke_withThrowingTerminalException() {
        Node node = getNode(instance2);
        assertThatThrownBy(() -> {
            invokeOnStableClusterParallel(
                    node.getNodeEngine(),
                    () -> new ExceptionThrowingOperation(new RuntimeException("expected")),
                    0
            ).join();
        }).isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("expected");
    }

    @Test
    public void testInvoke_withThrowingCompletionExceptionWithNullCause() {
        Node node = getNode(instance2);
        assertThatThrownBy(() -> {
            invokeOnStableClusterParallel(
                    node.getNodeEngine(),
                    () -> new ExceptionThrowingOperation(new CompletionException(null)),
                    0
            ).join();
        }).isInstanceOf(CompletionException.class)
                .hasCause(null);
    }

    @Test
    public void testInvoke_withThrowingIgnorableHazelcastInstanceNotActiveException() {
        assertExceptionIgnored(new HazelcastInstanceNotActiveException("expected"));
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

    private void assertExceptionIgnored(Exception exception) {
        Node node = getNode(instance3);
        Collection<UUID> uuids = invokeOnStableClusterParallel(
                node.getNodeEngine(),
                () -> new ExceptionThrowingOperation(exception),
                2
        ).join();

        Collection<UUID> expectedUuids = getUuidsOfInstances(instance1, instance2, instance3);
        assertThat(uuids)
                .containsExactlyInAnyOrderElementsOf(expectedUuids);
    }

    @Test
    public void testInvoke_withThrowingTopologyChangeException() {
        Node node = getNode(instance3);
        assertThatThrownBy(() -> {
            invokeOnStableClusterParallel(
                    node.getNodeEngine(),
                    () -> new ExceptionThrowingOperation(new ClusterTopologyChangedException("expected")),
                    20
            ).join();
        }).isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(HazelcastException.class)
                .hasStackTraceContaining("Cluster topology was not stable for 20 retries");
    }

    @Test
    public void testInvoke_whenNewMembersAreAdded() {
        Node node = getNode(instance1);
        InternalCompletableFuture<Collection<UUID>> future = invokeOnStableClusterParallel(
                node.getNodeEngine(),
                () -> new AwaitingOperation(4),
                2
        );

        HazelcastInstance instance4 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(4, instance1, instance2, instance3, instance4);

        Collection<UUID> expectedUuids = getUuidsOfInstances(instance1, instance2, instance3, instance4);

        Collection<UUID> uuids = future.join();
        assertThat(uuids)
                .containsExactlyInAnyOrderElementsOf(expectedUuids);
    }

    @Test
    public void testInvoke_whenNewMembersRemoved() {
        Node node = getNode(instance2);
        InternalCompletableFuture<Collection<UUID>> future = invokeOnStableClusterParallel(
                node.getNodeEngine(),
                () -> new AwaitingOperation(2),
                2
        );

        instance3.shutdown();
        assertClusterSizeEventually(2, instance1, instance2);

        Collection<UUID> expectedUuids = getUuidsOfInstances(instance1, instance2);

        Collection<UUID> uuids = future.join();
        assertThat(uuids)
                .containsExactlyInAnyOrderElementsOf(expectedUuids);
    }

    private static Collection<UUID> getUuidsOfInstances(HazelcastInstance... instances) {
        return Arrays.stream(instances)
                .map(instance -> instance.getLocalEndpoint().getUuid())
                .collect(Collectors.toList());
    }

    private static class NoOpOperation extends Operation {
    }

    private static class ExceptionThrowingOperation extends Operation {
        private Exception exception;

        ExceptionThrowingOperation() {
        }

        ExceptionThrowingOperation(Exception exception) {
            this.exception = exception;
        }

        @Override
        public void run() throws Exception {
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

    private static class AwaitingOperation extends Operation {

        private int expectedMemberCount;

        AwaitingOperation() {
        }

        AwaitingOperation(int expectedMemberCount) {
            this.expectedMemberCount = expectedMemberCount;
        }

        @Override
        public void run() throws Exception {
            while (getNodeEngine().getClusterService().getMembers().size() != expectedMemberCount) {
                Thread.sleep(100);
            }
            super.run();
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeInt(expectedMemberCount);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            expectedMemberCount = in.readInt();
        }
    }

    private static class InvokedMemberRecordingOperation extends Operation {
        private static final Map<String, Collection<UUID>> TEST_NAME_TO_INVOKED_MEMBER_UUIDS = new ConcurrentHashMap<>();
        private String testName;

        InvokedMemberRecordingOperation() {
        }

        InvokedMemberRecordingOperation(String testName) {
            this.testName = testName;
        }

        @Override
        public void run() throws Exception {
            TEST_NAME_TO_INVOKED_MEMBER_UUIDS
                    .computeIfAbsent(testName, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
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
}
