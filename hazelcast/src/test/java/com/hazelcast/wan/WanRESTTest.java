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

package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class WanRESTTest extends HazelcastTestSupport {
    private WanReplicationService wanServiceMock;
    private HTTPCommunicator communicator;

    @Before
    public void initInstance() {
        wanServiceMock = mock(WanReplicationService.class);
        HazelcastInstance instance = HazelcastInstanceFactory.newHazelcastInstance(getConfig(), randomName(),
                new WanServiceMockingNodeContext(wanServiceMock));
        communicator = new HTTPCommunicator(instance);
    }

    @Test
    public void pauseSuccess() throws Exception {
        assertSuccess(communicator.wanPausePublisher("atob", "B"));
        verify(wanServiceMock, times(1)).pause("atob", "B");
    }

    @Test
    public void stopSuccess() throws Exception {
        assertSuccess(communicator.wanStopPublisher("atob", "B"));
        verify(wanServiceMock, times(1)).stop("atob", "B");
    }

    @Test
    public void resumeSuccess() throws Exception {
        assertSuccess(communicator.wanResumePublisher("atob", "B"));
        verify(wanServiceMock, times(1)).resume("atob", "B");
    }

    @Test
    public void consistencyCheckSuccess() throws Exception {
        assertSuccess(communicator.wanMapConsistencyCheck("atob", "B", "mapName"));
        verify(wanServiceMock, times(1)).consistencyCheck("atob", "B", "mapName");
    }

    @Test
    public void pauseFail() throws Exception {
        doThrow(new RuntimeException("Error occurred"))
                .when(wanServiceMock)
                .pause("atob", "B");
        assertFail(communicator.wanPausePublisher("atob", "B"));
        verify(wanServiceMock, times(1)).pause("atob", "B");
    }

    @Test
    public void stopFail() throws Exception {
        doThrow(new RuntimeException("Error occurred"))
                .when(wanServiceMock)
                .stop("atob", "B");
        assertFail(communicator.wanStopPublisher("atob", "B"));
        verify(wanServiceMock, times(1)).stop("atob", "B");
    }

    @Test
    public void resumeFail() throws Exception {
        doThrow(new RuntimeException("Error occurred"))
                .when(wanServiceMock)
                .resume("atob", "B");
        assertFail(communicator.wanResumePublisher("atob", "B"));
        verify(wanServiceMock, times(1)).resume("atob", "B");
    }

    @Test
    public void consistencyCheckFail() throws Exception {
        doThrow(new RuntimeException("Error occurred"))
                .when(wanServiceMock)
                .consistencyCheck("atob", "B", "mapName");
        assertFail(communicator.wanMapConsistencyCheck("atob", "B", "mapName"));
        verify(wanServiceMock, times(1)).consistencyCheck("atob", "B", "mapName");
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig()
                .setProperty(GroupProperty.REST_ENABLED.getName(), "true");
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig()
                  .setEnabled(false);
        joinConfig.getTcpIpConfig()
                  .setEnabled(true)
                  .addMember("127.0.0.1");
        return config;
    }

    @After
    public void cleanup() {
        HazelcastInstanceFactory.shutdownAll();
    }

    private void assertFail(String jsonResult) {
        JsonObject result = Json.parse(jsonResult).asObject();
        assertEquals("fail", result.getString("status", null));
    }

    private void assertSuccess(String jsonResult) {
        JsonObject result = Json.parse(jsonResult).asObject();
        assertEquals("success", result.getString("status", null));
    }
}
