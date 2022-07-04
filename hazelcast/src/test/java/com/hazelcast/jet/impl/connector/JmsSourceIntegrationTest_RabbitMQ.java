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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.rabbitmq.jms.admin.RMQConnectionFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.RabbitMQContainer;

import javax.jms.ConnectionFactory;

import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;

@Category({NightlyTest.class, ParallelJVMTest.class, IgnoreInJenkinsOnWindows.class})
public class JmsSourceIntegrationTest_RabbitMQ extends JmsSourceIntegrationTestBase {

    @ClassRule
    public static RabbitMQContainer container = new RabbitMQContainer("rabbitmq:3.8");

    private static final SupplierEx<ConnectionFactory> FACTORY_SUPPLIER = () -> {
        RMQConnectionFactory f = new RMQConnectionFactory();
        f.setUri(container.getAmqpUrl());
        return f;
    };

    @BeforeClass
    public static void beforeClassCheckDocker() {
        assumeDockerEnabled();
    }

    @Override
    protected SupplierEx<ConnectionFactory> getConnectionFactory() {
        return FACTORY_SUPPLIER;
    }

    @Override
    public void stressTest_exactlyOnce_forceful_durableTopic() {
        // ignore this test for RabbitMQ, it doesn't support JMS 2.0
    }
}
