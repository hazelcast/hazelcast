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
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQResource;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import javax.jms.ConnectionFactory;

@Ignore // ignored due to https://issues.apache.org/jira/browse/ARTEMIS-2735
@Category({QuickTest.class, ParallelJVMTest.class})
public class JmsSourceIntegrationTest_ActiveMqArtemis extends JmsSourceIntegrationTestBase {

    @ClassRule
    public static EmbeddedActiveMQResource broker = new EmbeddedActiveMQResource();

    private static final SupplierEx<ConnectionFactory> FACTORY_SUPPLIER =
            () -> new ActiveMQConnectionFactory(broker.getVmURL());

    @Override
    protected SupplierEx<ConnectionFactory> getConnectionFactory() {
        return FACTORY_SUPPLIER;
    }
}
