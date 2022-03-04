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

package com.hazelcast.test.metrics;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.util.StringUtil;
import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test rule that records the metrics collected on the Hazelcast instances
 * during a test run and dumps the recorded metrics if the given test
 * fails. By default the last 10 metrics collection is recorded. After
 * that, the rule starts overwriting the oldest recording. The metrics are
 * recorded in the memory in compressed format, therefore, it doesn't
 * increase the used heap noticeably.
 */
public class MetricsRule implements TestRule {
    private Map<String, TestMetricPublisher> publishers = new ConcurrentHashMap<>();
    private volatile boolean enabled = true;
    private volatile int slots = 10;

    public MetricsRule disable() {
        enabled = false;
        return this;
    }

    public MetricsRule slots(int slots) {
        this.slots = slots;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } catch (AssumptionViolatedException e) {
                    // represents expected exceptions, no need to take an action.
                } catch (Throwable t) {
                    StringBuilder sb = new StringBuilder();
                    publishers.forEach((instanceName, publisher) -> publisher.dumpRecordings(instanceName, sb));

                    String metricsString = sb.toString();
                    if (!StringUtil.isNullOrEmptyAfterTrim(metricsString)) {
                        System.out.println("\nMetrics recorded during the test:" + metricsString);
                    } else {
                        System.out.println("\nNo metrics recorded during the test");
                    }

                    throw t;
                }
            }
        };
    }

    public MetricsPublisher getMetricsPublisher(HazelcastInstance instance) {
        TestMetricPublisher publisher = new TestMetricPublisher(slots);
        publishers.put(instance.getName(), publisher);
        return publisher;
    }
}
