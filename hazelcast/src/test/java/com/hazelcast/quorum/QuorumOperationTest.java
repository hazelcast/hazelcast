/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum;

import com.hazelcast.concurrent.lock.operations.AbstractLockOperation;
import com.hazelcast.concurrent.lock.operations.GetLockCountOperation;
import com.hazelcast.concurrent.lock.operations.GetRemainingLeaseTimeOperation;
import com.hazelcast.concurrent.lock.operations.IsLockedOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.operation.AwaitMapFlushOperation;
import com.hazelcast.map.impl.operation.IsPartitionLoadedOperation;
import com.hazelcast.map.impl.operation.NotifyMapFlushOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ServiceLoader;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.fail;

/**
 * Asserts that operations are implementing or not implementing {@link MutatingOperation} or {@link ReadonlyOperation},
 * depending on a set of naming rules.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QuorumOperationTest {

    private static final ILogger LOGGER = Logger.getLogger(QuorumOperationTest.class);

    /**
     * List of trigger words in internal class names, which are not allowed to implement
     * {@link MutatingOperation} or {@link ReadonlyOperation}.
     */
    private static final Collection<String> INTERNAL_CLASS_NAMES = asList(
            "merge", "backup",
            "replication", "migration",
            "postjoin", "rollback", "onjoin",
            "detachmember", "putresult"
    );

    /**
     * List of trigger words in class names of {@link MutatingOperation}.
     */
    private static final Collection<String> MUTATING_CLASS_NAMES = asList(
            "put", "set", "replace", "update",
            "add", "reduce", "alter", "apply",
            "remove", "delete", "evict",
            "offer", "poll", "drain",
            "init", "acquire", "release", "detach",
            "aggregate",
            "countdown",
            "entryoperation", "entrywithpredicateoperation",
            "callable", "task", "dispose", "cancel", "shutdown",
            "lock", "signal",
            "prepare", "commit",
            "load", "flush",
            "clear", "destroy",
            "increase"
    );

    /**
     * List of trigger words in class names of {@link ReadonlyOperation}.
     */
    private static final Collection<String> READONLY_CLASS_NAMES = asList(
            "get", "retrieve", "fetch",
            "query", "contains", "peek",
            "estimate",
            "iterator",
            "available", "await",
            "size", "isempty", "isnull"
    );

    /**
     * List of of {@link ReadonlyOperation} classes, which are falsely triggered by {@link #MUTATING_CLASS_NAMES}.
     */
    private static final List<? extends Class<? extends Operation>> FORCED_READONLY_CLASSES = asList(
            AwaitMapFlushOperation.class,
            NotifyMapFlushOperation.class,
            IsPartitionLoadedOperation.class,
            IsLockedOperation.class,
            GetLockCountOperation.class,
            GetRemainingLeaseTimeOperation.class
    );

    /**
     * Operations in these packages should never require a quorum, so they are not allowed to implement
     * {@link MutatingOperation} or {@link ReadonlyOperation}.
     */
    private static final Collection<String> INTERNAL_PACKAGES = asList(
            "com.hazelcast.internal.cluster.impl.operations.",
            "com.hazelcast.internal.dynamicconfig.",
            "com.hazelcast.internal.management.operation.",
            "com.hazelcast.internal.usercodedeployment.impl.operation.",
            "com.hazelcast.spi.impl.eventservice.impl.operations",
            "com.hazelcast.spi.impl.operationservice.impl.operations.",
            "com.hazelcast.spi.impl.proxyservice.impl.operations"
    );

    /**
     * These data structures don't implement quorum, so they are not allowed to implement
     * {@link MutatingOperation} or {@link ReadonlyOperation}.
     */
    private static final Collection<String> NO_QUORUM_PACKAGES = asList(
            "com.hazelcast.flakeidgen.impl.",
            "com.hazelcast.mapreduce.impl.operation.",
            "com.hazelcast.topic.impl.",
            "com.hazelcast.transaction.impl.xa.operations."
    );

    private static final String FACTORY_ID = "com.hazelcast.DataSerializerHook";

    private static final String MUTATING_OP_NAME = MutatingOperation.class.getSimpleName();
    private static final String READ_ONLY_OP_NAME = ReadonlyOperation.class.getSimpleName();

    @Test
    public void assertThatInternalOperationsAreNotQuorumDependent() throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Iterator<DataSerializerHook> hooks = ServiceLoader.iterator(DataSerializerHook.class, FACTORY_ID, classLoader);
        while (hooks.hasNext()) {
            DataSerializerHook hook = hooks.next();
            LOGGER.info("Testing " + hook.getClass().getSimpleName() + "...");
            DataSerializableFactory factory = hook.createFactory();
            int typeId = 0;
            while (true) {
                IdentifiedDataSerializable ids = createIDS(factory, typeId++);
                if (ids == null) {
                    break;
                }

                Class<? extends IdentifiedDataSerializable> clazz = ids.getClass();
                String className = clazz.getName();
                String name = clazz.getSimpleName().toLowerCase();
                LOGGER.info(clazz.getSimpleName());

                boolean shouldBeMutatingOperation = false;
                boolean shouldBeReadonlyOperation = false;
                if (!(ids instanceof Operation)
                        || ids instanceof UrgentSystemOperation
                        || matches(INTERNAL_CLASS_NAMES, name)
                        || matches(INTERNAL_PACKAGES, className)
                        || matches(NO_QUORUM_PACKAGES, className)) {
                    // no, urgent, internal or no quorum operations
                    shouldBeMutatingOperation = false;
                    shouldBeReadonlyOperation = false;
                } else if (ids instanceof AbstractLockOperation
                        || matches(MUTATING_CLASS_NAMES, name)) {
                    // mutating operations
                    if (isForcedReadOnly(className)) {
                        shouldBeReadonlyOperation = true;
                    } else {
                        shouldBeMutatingOperation = true;
                    }
                } else if (matches(READONLY_CLASS_NAMES, name)) {
                    // read-only operations
                    shouldBeReadonlyOperation = true;
                } else {
                    fail(className + " doesn't match any criteria!");
                }

                // asserts
                if (ids instanceof MutatingOperation) {
                    if (!shouldBeMutatingOperation) {
                        fail(className + " implements " + MUTATING_OP_NAME);
                    }
                } else if (shouldBeMutatingOperation) {
                    fail(className + " should implement " + MUTATING_OP_NAME);
                }
                if (ids instanceof ReadonlyOperation) {
                    if (!shouldBeReadonlyOperation) {
                        fail(className + " implements " + READ_ONLY_OP_NAME);
                    }
                } else if (shouldBeReadonlyOperation) {
                    fail(className + " should implement " + READ_ONLY_OP_NAME);
                }
            }
        }
    }

    private static IdentifiedDataSerializable createIDS(DataSerializableFactory factory, int typeId) {
        try {
            return factory.create(typeId);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static boolean matches(Collection<String> collection, String name) {
        for (String triggerWord : collection) {
            if (name.contains(triggerWord)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isForcedReadOnly(String name) {
        for (Class whitelistedClass : FORCED_READONLY_CLASSES) {
            if (name.equals(whitelistedClass.getName())) {
                return true;
            }
        }
        return false;
    }
}
