/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.bounce;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.bounce.BounceTestConfiguration.DriverType;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

/**
 * JUnit 5 counterpart of {@link BounceMemberRule}, with the same configuration and test helpers.
 * Register an instance using {@code @RegisterExtension}:
 * <pre>{@code
 * @RegisterExtension
 * final BounceMemberExtension bounce = BounceMemberExtension.with(new Config())
 *         .clusterSize(3)
 *         .driverType(BounceTestConfiguration.DriverType.MEMBER)
 *         .build();
 * }</pre>
 * The cluster and drivers are created before {@code @BeforeEach}. Members bounce only during the
 * test method, stopping before {@code @AfterEach}. The cluster is torn down after {@code @AfterEach},
 * including when setup or the test fails. Each test gets a fresh cluster and task execution state.
 * Like the rule, an extension instance must not be shared by concurrently executing tests.
 * Custom {@link DriverFactory} implementations receive the underlying rule.
 */
public class BounceMemberExtension implements BeforeEachCallback, BeforeTestExecutionCallback,
        AfterTestExecutionCallback, AfterEachCallback {

    public static final long STALENESS_DETECTOR_DISABLED = BounceMemberRule.STALENESS_DETECTOR_DISABLED;

    private BounceMemberRule delegate;

    private BounceMemberExtension(BounceMemberRule delegate) {
        this.delegate = delegate;
    }

    public static Builder with(Config memberConfig) {
        return new Builder(BounceMemberRule.with(memberConfig));
    }

    public static Builder with(Supplier<Config> memberConfigSupplier) {
        return new Builder(BounceMemberRule.with(memberConfigSupplier));
    }

    @Override
    public void beforeEach(@NonNull ExtensionContext context) {
        delegate = new BounceMemberRule(delegate.getBounceTestConfig());
        delegate.setup();
    }

    @Override
    public void beforeTestExecution(@NonNull ExtensionContext context) {
        delegate.startBouncing();
    }

    @Override
    public void afterTestExecution(@NonNull ExtensionContext context) {
        delegate.stopBouncing();
    }

    @Override
    public void afterEach(@NonNull ExtensionContext context) {
        try {
            delegate.stopBouncing();
        } finally {
            delegate.tearDown();
        }
    }

    public HazelcastInstance getSteadyMember() {
        return delegate.getSteadyMember();
    }

    public HazelcastInstance getNextTestDriver() {
        return delegate.getNextTestDriver();
    }

    public TestHazelcastInstanceFactory getFactory() {
        return delegate.getFactory();
    }

    public BounceTestConfiguration getBounceTestConfig() {
        return delegate.getBounceTestConfig();
    }

    public AtomicReferenceArray<HazelcastInstance> getMembers() {
        return delegate.getMembers();
    }

    public HazelcastInstance[] getMembersSnapshot() {
        return delegate.getMembersSnapshot();
    }

    public AtomicReferenceArray<HazelcastInstance> getTestDrivers() {
        return delegate.getTestDrivers();
    }

    public void testRepeatedly(int concurrency, Runnable task, long durationSeconds) {
        delegate.testRepeatedly(concurrency, task, durationSeconds);
    }

    public void testRepeatedly(Runnable[] tasks, long durationSeconds) {
        delegate.testRepeatedly(tasks, durationSeconds);
    }

    public void test(Runnable[] tasks) {
        delegate.test(tasks);
    }

    public static class Builder {

        private final BounceMemberRule.Builder delegate;

        private Builder(BounceMemberRule.Builder delegate) {
            this.delegate = delegate;
        }

        public BounceMemberExtension build() {
            return new BounceMemberExtension(delegate.build());
        }

        public Builder clusterSize(int clusterSize) {
            delegate.clusterSize(clusterSize);
            return this;
        }

        public Builder driverType(DriverType testDriverType) {
            delegate.driverType(testDriverType);
            return this;
        }

        public Builder driverCount(int count) {
            delegate.driverCount(count);
            return this;
        }

        public Builder bouncingIntervalSeconds(int bouncingIntervalSeconds) {
            delegate.bouncingIntervalSeconds(bouncingIntervalSeconds);
            return this;
        }

        public Builder maximumStalenessSeconds(int maximumStalenessSeconds) {
            delegate.maximumStalenessSeconds(maximumStalenessSeconds);
            return this;
        }

        public Builder driverFactory(DriverFactory driverFactory) {
            delegate.driverFactory(driverFactory);
            return this;
        }

        public Builder useTerminate(boolean value) {
            delegate.useTerminate(value);
            return this;
        }

        public Builder avoidOverlappingTerminations(boolean value) {
            delegate.avoidOverlappingTerminations(value);
            return this;
        }

        public Builder noSteadyMember() {
            delegate.noSteadyMember();
            return this;
        }

        public Builder shouldFailOnIncompleteTask() {
            delegate.shouldFailOnIncompleteTask();
            return this;
        }

        public Builder testTaskTimeoutSecs(int testTaskTimeoutSecs) {
            delegate.testTaskTimeoutSecs(testTaskTimeoutSecs);
            return this;
        }
    }
}
