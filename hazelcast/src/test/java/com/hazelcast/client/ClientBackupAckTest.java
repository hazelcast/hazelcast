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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientBackupAckTest extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testBackupAckToClientIsEnabled_byDefault() {
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(isEnabled(client));
    }

    @Test
    public void testBackupAckToClientIsEnabled() {
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBackupAckToClientEnabled(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(isEnabled(client));
    }

    @Test
    public void testBackupAckToClientIsDisabled() {
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBackupAckToClientEnabled(false);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertFalse(isEnabled(client));
    }

    private boolean isEnabled(HazelcastInstance client) {
        Collection<EventHandler> values = getAllEventHandlers(client).values();
        for (EventHandler value : values) {
            if (value instanceof ClientInvocationServiceImpl.BackupEventHandler) {
                return true;
            }
        }
        return false;
    }
}
