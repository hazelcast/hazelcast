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

package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.impl.WanReplicationService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class WanRESTTest extends HazelcastTestSupport {
    private WanReplicationService wanServiceMock;
    private TestHazelcastInstanceFactory factory;
    private HTTPCommunicator communicator;

    @Rule
    public final OverridePropertyRule overridePropertyRule = set(HAZELCAST_TEST_USE_NETWORK, "true");

    @Before
    public void setupFactoryAndMock() {
        wanServiceMock = mock(WanReplicationService.class);
        factory = new CustomNodeExtensionTestInstanceFactory(
                node -> new WanServiceMockingDefaultNodeExtension(node, wanServiceMock));
    }

    @Test
    public void pauseSuccess() throws Exception {
        startInstance();
        assertSuccess(communicator.wanPausePublisher(getConfig().getClusterName(), "", "atob", "B"));
        verify(wanServiceMock, times(1)).pause("atob", "B");
    }

    @Test
    public void stopSuccess() throws Exception {
        startInstance();
        assertSuccess(communicator.wanStopPublisher(getConfig().getClusterName(), "", "atob", "B"));
        verify(wanServiceMock, times(1)).stop("atob", "B");
    }

    @Test
    public void resumeSuccess() throws Exception {
        startInstance();
        assertSuccess(communicator.wanResumePublisher(getConfig().getClusterName(), "", "atob", "B"));
        verify(wanServiceMock, times(1)).resume("atob", "B");
    }

    @Test
    public void consistencyCheckSuccess() throws Exception {
        UUID expectedUuid = UUID.randomUUID();
        when(wanServiceMock.consistencyCheck("atob", "B", "mapName"))
                .thenReturn(expectedUuid);
        startInstance();

        String result = communicator.wanMapConsistencyCheck(getConfig().getClusterName(), "", "atob", "B", "mapName");
        assertSuccess(result);
        assertUuid(result, expectedUuid);
        verify(wanServiceMock, times(1)).consistencyCheck("atob", "B", "mapName");
    }

    @Test
    public void syncSuccess() throws Exception {
        UUID expectedUuid = UUID.randomUUID();
        when(wanServiceMock.syncMap("atob", "B", "mapName")).thenReturn(expectedUuid);
        startInstance();

        String result = communicator.syncMapOverWAN(getConfig().getClusterName(), "", "atob", "B", "mapName");
        assertSuccess(result);
        assertUuid(result, expectedUuid);
        verify(wanServiceMock, times(1)).syncMap("atob", "B", "mapName");
    }

    @Test
    public void syncAllSuccess() throws Exception {
        UUID expectedUuid = UUID.randomUUID();
        when(wanServiceMock.syncAllMaps("atob", "B")).thenReturn(expectedUuid);
        startInstance();

        String result = communicator.syncMapsOverWAN(getConfig().getClusterName(), "", "atob", "B");
        assertSuccess(result);
        assertUuid(result, expectedUuid);
        verify(wanServiceMock, times(1)).syncAllMaps("atob", "B");
    }

    @Test
    public void pauseFail() throws Exception {
        doThrow(new RuntimeException("Error occurred"))
                .when(wanServiceMock)
                .pause("atob", "B");
        startInstance();

        assertFail(communicator.wanPausePublisher(getConfig().getClusterName(), "", "atob", "B"));
        verify(wanServiceMock, times(1)).pause("atob", "B");
    }

    @Test
    public void stopFail() throws Exception {
        doThrow(new RuntimeException("Error occurred"))
                .when(wanServiceMock)
                .stop("atob", "B");
        startInstance();

        assertFail(communicator.wanStopPublisher(getConfig().getClusterName(), "", "atob", "B"));
        verify(wanServiceMock, times(1)).stop("atob", "B");
    }

    @Test
    public void resumeFail() throws Exception {
        doThrow(new RuntimeException("Error occurred"))
                .when(wanServiceMock)
                .resume("atob", "B");
        startInstance();

        assertFail(communicator.wanResumePublisher(getConfig().getClusterName(), "", "atob", "B"));
        verify(wanServiceMock, times(1)).resume("atob", "B");
    }

    @Test
    public void consistencyCheckFail() throws Exception {
        doThrow(new RuntimeException("Error occurred"))
                .when(wanServiceMock)
                .consistencyCheck("atob", "B", "mapName");
        startInstance();

        assertFail(communicator.wanMapConsistencyCheck(getConfig().getClusterName(), "", "atob", "B", "mapName"));
        verify(wanServiceMock, times(1)).consistencyCheck("atob", "B", "mapName");
    }

    @Test
    public void syncFail() throws Exception {
        doThrow(new RuntimeException("Error occurred"))
                .when(wanServiceMock)
                .syncMap("atob", "B", "mapName");
        startInstance();

        assertFail(communicator.syncMapOverWAN(getConfig().getClusterName(), "", "atob", "B", "mapName"));
        verify(wanServiceMock, times(1)).syncMap("atob", "B", "mapName");
    }

    private void startInstance() {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());
        communicator = new HTTPCommunicator(instance);
    }

    @Test
    public void syncAllFail() throws Exception {
        doThrow(new RuntimeException("Error occurred"))
                .when(wanServiceMock)
                .syncAllMaps("atob", "B");
        startInstance();

        assertFail(communicator.syncMapsOverWAN(getConfig().getClusterName(), "", "atob", "B"));
        verify(wanServiceMock, times(1)).syncAllMaps("atob", "B");
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        RestApiConfig restApiConfig = config.getNetworkConfig().getRestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.enableGroups(RestEndpointGroup.WAN);
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
        factory.shutdownAll();
    }

    private void assertFail(String jsonResult) {
        JsonObject result = Json.parse(jsonResult).asObject();
        assertEquals("fail", result.getString("status", null));
    }

    private void assertSuccess(String jsonResult) {
        JsonObject result = Json.parse(jsonResult).asObject();
        assertEquals("success", result.getString("status", null));
    }

    private void assertUuid(String jsonResult, UUID expectedUuid) {
        JsonObject result = Json.parse(jsonResult).asObject();
        assertEquals(expectedUuid.toString(), result.getString("uuid", null));
    }
}
