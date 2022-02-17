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

package com.hazelcast.cluster;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.internal.util.ExceptionUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.fail;

public abstract class AbstractClusterNullTest extends HazelcastTestSupport {

    @Test
    public void testNullability() {
        assertThrowsNPE(c -> c.addMembershipListener(null));
        assertThrowsNPE(c -> c.removeMembershipListener(null));
        assertThrowsNPE(c -> c.changeClusterState(null));

        assertThrowsNPE(c -> c.changeClusterState(null, TransactionOptions.getDefault()));
        assertThrowsNPE(c -> c.changeClusterState(ClusterState.PASSIVE, null));

        assertThrowsNPE(c -> c.changeClusterVersion(null));
        assertThrowsNPE(c -> c.changeClusterVersion(null, TransactionOptions.getDefault()));
        assertThrowsNPE(c -> c.changeClusterVersion(Versions.V4_0, null));
    }

    private void assertThrowsNPE(ConsumerEx<Cluster> method) {
        assertThrows(NullPointerException.class, method);
    }

    private void assertThrows(Class<? extends Exception> expectedExceptionClass,
                              ConsumerEx<Cluster> method) {
        try {
            method.accept(getDriver().getCluster());
            fail("Expected " + expectedExceptionClass
                    + " but there was no exception!");
        } catch (Exception e) {
            Assert.assertSame(expectedExceptionClass, e.getClass());
        }
    }

    @FunctionalInterface
    public interface ConsumerEx<T> extends Consumer<T> {
        void acceptEx(T t) throws Exception;

        @Override
        default void accept(T t) {
            try {
                acceptEx(t);
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    protected abstract HazelcastInstance getDriver();
}
