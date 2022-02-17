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

package com.hazelcast.internal.ascii;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.session.CPSession;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.ascii.HTTPCommunicator.ConnectionResponse;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cp.CPGroup.DEFAULT_GROUP_NAME;
import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.cp.internal.HazelcastRaftTestSupport.waitUntilCPDiscoveryCompleted;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class RestCPSubsystemTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Config config = new Config();
    private String clusterName = config.getClusterName();

    @Before
    public void setup() {
        RestApiConfig restApiConfig = new RestApiConfig()
                .setEnabled(true)
                .enableGroups(RestEndpointGroup.CP);
        config.getNetworkConfig().setRestApiConfig(restApiConfig);

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).clear().addMember("127.0.0.1");

        config.getCPSubsystemConfig().setCPMemberCount(3);
    }

    @After
    public void tearDown() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void test_getCPGroupIds() throws IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        instance1.getCPSubsystem().getAtomicLong("long1").set(5);
        instance1.getCPSubsystem().getAtomicLong("long1@custom").set(5);

        HTTPCommunicator communicator = new HTTPCommunicator(instance1);

        ConnectionResponse response = communicator.getCPGroupIds();

        assertEquals(200, response.responseCode);

        JsonArray responseArr = (JsonArray) Json.parse(response.response);

        boolean metadataCPGroupExists = false;
        boolean defaultCPGroupExists = false;
        boolean customCPGroupExists = false;

        for (JsonValue val : responseArr) {
            JsonObject obj = (JsonObject) val;
            String name = obj.getString("name", "");
            if (CPGroup.DEFAULT_GROUP_NAME.equals(name)) {
                defaultCPGroupExists = true;
            } else if (METADATA_CP_GROUP_NAME.equals(name)) {
                metadataCPGroupExists = true;
            } else if ("custom".equals(name)) {
                customCPGroupExists = true;
            }
        }

        assertTrue(metadataCPGroupExists);
        assertTrue(defaultCPGroupExists);
        assertTrue(customCPGroupExists);
    }

    @Test
    public void test_getMetadataCPGroupByName() throws IOException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        waitUntilCPDiscoveryCompleted(instance1, instance2, instance3);

        HTTPCommunicator communicator = new HTTPCommunicator(instance1);

        ConnectionResponse response = communicator.getCPGroupByName(METADATA_CP_GROUP_NAME);

        assertEquals(200, response.responseCode);

        CPMember cpMember1 = instance1.getCPSubsystem().getLocalCPMember();
        CPMember cpMember2 = instance2.getCPSubsystem().getLocalCPMember();
        CPMember cpMember3 = instance3.getCPSubsystem().getLocalCPMember();

        boolean cpMember1Found = false;
        boolean cpMember2Found = false;
        boolean cpMember3Found = false;

        JsonObject json = (JsonObject) Json.parse(response.response);

        assertEquals(METADATA_CP_GROUP_NAME, ((JsonObject) json.get("id")).getString("name", ""));
        assertEquals(CPGroupStatus.ACTIVE.name(), json.getString("status", ""));

        for (JsonValue val : (JsonArray) json.get("members")) {
            JsonObject mem = (JsonObject) val;
            cpMember1Found |= cpMember1.getUuid().equals(UUID.fromString(mem.getString("uuid", "")));
            cpMember2Found |= cpMember2.getUuid().equals(UUID.fromString(mem.getString("uuid", "")));
            cpMember3Found |= cpMember3.getUuid().equals(UUID.fromString(mem.getString("uuid", "")));
        }

        assertTrue(cpMember1Found);
        assertTrue(cpMember2Found);
        assertTrue(cpMember3Found);
    }

    @Test
    public void test_getDefaultCPGroupByName() throws IOException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        waitUntilCPDiscoveryCompleted(instance1, instance2, instance3);

        instance1.getCPSubsystem().getAtomicLong("long1").set(5);

        HTTPCommunicator communicator = new HTTPCommunicator(instance1);

        ConnectionResponse response = communicator.getCPGroupByName(DEFAULT_GROUP_NAME);

        assertEquals(200, response.responseCode);

        CPMember cpMember1 = instance1.getCPSubsystem().getLocalCPMember();
        CPMember cpMember2 = instance2.getCPSubsystem().getLocalCPMember();
        CPMember cpMember3 = instance3.getCPSubsystem().getLocalCPMember();

        boolean cpMember1Found = false;
        boolean cpMember2Found = false;
        boolean cpMember3Found = false;

        JsonObject json = (JsonObject) Json.parse(response.response);

        assertEquals(DEFAULT_GROUP_NAME, ((JsonObject) json.get("id")).getString("name", ""));
        assertEquals(CPGroupStatus.ACTIVE.name(), json.getString("status", ""));

        for (JsonValue val : (JsonArray) json.get("members")) {
            JsonObject mem = (JsonObject) val;
            cpMember1Found |= cpMember1.getUuid().equals(UUID.fromString(mem.getString("uuid", "")));
            cpMember2Found |= cpMember2.getUuid().equals(UUID.fromString(mem.getString("uuid", "")));
            cpMember3Found |= cpMember3.getUuid().equals(UUID.fromString(mem.getString("uuid", "")));
        }

        assertTrue(cpMember1Found);
        assertTrue(cpMember2Found);
        assertTrue(cpMember3Found);
    }

    @Test
    public void test_getCustomCPGroupByName() throws IOException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        waitUntilCPDiscoveryCompleted(instance1, instance2, instance3);

        instance1.getCPSubsystem().getAtomicLong("long1@custom").set(5);

        HTTPCommunicator communicator = new HTTPCommunicator(instance1);

        ConnectionResponse response = communicator.getCPGroupByName("custom");

        assertEquals(200, response.responseCode);

        CPMember cpMember1 = instance1.getCPSubsystem().getLocalCPMember();
        CPMember cpMember2 = instance2.getCPSubsystem().getLocalCPMember();
        CPMember cpMember3 = instance3.getCPSubsystem().getLocalCPMember();

        boolean cpMember1Found = false;
        boolean cpMember2Found = false;
        boolean cpMember3Found = false;

        JsonObject json = (JsonObject) Json.parse(response.response);

        assertEquals("custom", ((JsonObject) json.get("id")).getString("name", ""));
        assertEquals(CPGroupStatus.ACTIVE.name(), json.getString("status", ""));

        for (JsonValue val : (JsonArray) json.get("members")) {
            JsonObject mem = (JsonObject) val;
            cpMember1Found |= cpMember1.getUuid().equals(UUID.fromString(mem.getString("uuid", "")));
            cpMember2Found |= cpMember2.getUuid().equals(UUID.fromString(mem.getString("uuid", "")));
            cpMember3Found |= cpMember3.getUuid().equals(UUID.fromString(mem.getString("uuid", "")));
        }

        assertTrue(cpMember1Found);
        assertTrue(cpMember2Found);
        assertTrue(cpMember3Found);
    }

    @Test
    public void test_getCPGroupByInvalidName() throws IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        HTTPCommunicator communicator = new HTTPCommunicator(instance1);

        ConnectionResponse response = communicator.getCPGroupByName("custom");

        assertEquals(404, response.responseCode);
    }

    @Test
    public void test_getLocalCPMember() throws IOException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance4 = Hazelcast.newHazelcastInstance(config);

        waitUntilCPDiscoveryCompleted(instance1, instance2, instance3, instance4);

        ConnectionResponse response1 = new HTTPCommunicator(instance1).getLocalCPMember();
        ConnectionResponse response2 = new HTTPCommunicator(instance2).getLocalCPMember();
        ConnectionResponse response3 = new HTTPCommunicator(instance3).getLocalCPMember();
        ConnectionResponse response4 = new HTTPCommunicator(instance4).getLocalCPMember();

        assertEquals(200, response1.responseCode);
        assertEquals(200, response2.responseCode);
        assertEquals(200, response3.responseCode);
        assertEquals(404, response4.responseCode);

        CPMember cpMember1 = instance1.getCPSubsystem().getLocalCPMember();
        CPMember cpMember2 = instance2.getCPSubsystem().getLocalCPMember();
        CPMember cpMember3 = instance3.getCPSubsystem().getLocalCPMember();

        assertEquals(cpMember1.getUuid(), UUID.fromString(((JsonObject) Json.parse(response1.response)).getString("uuid", "")));
        assertEquals(cpMember2.getUuid(), UUID.fromString(((JsonObject) Json.parse(response2.response)).getString("uuid", "")));
        assertEquals(cpMember3.getUuid(), UUID.fromString(((JsonObject) Json.parse(response3.response)).getString("uuid", "")));
    }

    @Test
    public void test_getCPMembers() throws IOException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        waitUntilCPDiscoveryCompleted(instance1, instance2, instance3);

        ConnectionResponse response = new HTTPCommunicator(instance1).getCPMembers();

        CPMember cpMember1 = instance1.getCPSubsystem().getLocalCPMember();
        CPMember cpMember2 = instance2.getCPSubsystem().getLocalCPMember();
        CPMember cpMember3 = instance3.getCPSubsystem().getLocalCPMember();

        boolean cpMember1Found = false;
        boolean cpMember2Found = false;
        boolean cpMember3Found = false;

        JsonArray arr = (JsonArray) Json.parse(response.response);

        for (JsonValue val : arr) {
            JsonObject mem = (JsonObject) val;
            cpMember1Found |= cpMember1.getUuid().equals(UUID.fromString(mem.getString("uuid", "")));
            cpMember2Found |= cpMember2.getUuid().equals(UUID.fromString(mem.getString("uuid", "")));
            cpMember3Found |= cpMember3.getUuid().equals(UUID.fromString(mem.getString("uuid", "")));
        }

        assertTrue(cpMember1Found);
        assertTrue(cpMember2Found);
        assertTrue(cpMember3Found);
    }

    @Test
    public void test_forceDestroyDefaultCPGroup() throws IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        IAtomicLong long1 = instance1.getCPSubsystem().getAtomicLong("long1");

        ConnectionResponse response = new HTTPCommunicator(instance1).forceDestroyCPGroup(DEFAULT_GROUP_NAME, clusterName, null);

        assertEquals(200, response.responseCode);

        exception.expect(CPGroupDestroyedException.class);

        long1.set(5);
    }

    @Test
    public void test_forceDestroyCPGroup_withInvalidCredentials() throws IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        instance1.getCPSubsystem().getAtomicLong("long1");

        ConnectionResponse response = new HTTPCommunicator(instance1).forceDestroyCPGroup(DEFAULT_GROUP_NAME, "x", "x");

        assertEquals(403, response.responseCode);
    }

    @Test
    public void test_forceDestroyMETADATACPGroup() throws IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        ConnectionResponse  response = new HTTPCommunicator(instance1).forceDestroyCPGroup(METADATA_CP_GROUP_NAME, clusterName, null);

        assertEquals(400, response.responseCode);
    }

    @Test
    public void test_forceDestroyInvalidCPGroup() throws IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        ConnectionResponse response = new HTTPCommunicator(instance1).forceDestroyCPGroup("custom", clusterName, null);

        assertEquals(400, response.responseCode);
    }

    @Test
    public void test_removeCPMember() throws IOException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        waitUntilCPDiscoveryCompleted(instance1, instance2, instance3);

        CPMember crashedCPMember = instance3.getCPSubsystem().getLocalCPMember();
        instance3.getLifecycleService().terminate();

        assertClusterSizeEventually(2, instance1, instance2);

        ConnectionResponse response = new HTTPCommunicator(instance1).removeCPMember(crashedCPMember.getUuid(), clusterName, null);

        assertEquals(200, response.responseCode);
    }

    @Test
    public void test_removeCPMember_withInvalidCredentials() throws IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        waitUntilCPDiscoveryCompleted(instance1, instance2, instance3);

        CPMember crashedCPMember = instance3.getCPSubsystem().getLocalCPMember();
        instance3.getLifecycleService().terminate();

        assertClusterSizeEventually(2, instance1, instance2);

        ConnectionResponse response = new HTTPCommunicator(instance1).removeCPMember(crashedCPMember.getUuid(), "x", "x");

        assertEquals(403, response.responseCode);
    }

    @Test
    public void test_removeInvalidCPMember() throws IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        ConnectionResponse response = new HTTPCommunicator(instance1).removeCPMember(newUnsecureUUID(), clusterName, null);

        assertEquals(400, response.responseCode);
    }

    @Test
    public void test_removeCPMemberFromNonMaster() throws IOException {
        Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        waitUntilCPDiscoveryCompleted(instance2, instance3);

        CPMember crashedCPMember = instance2.getCPSubsystem().getLocalCPMember();
        instance2.getLifecycleService().terminate();

        ConnectionResponse response = new HTTPCommunicator(instance3).removeCPMember(crashedCPMember.getUuid(), clusterName, null);

        assertEquals(200, response.responseCode);
    }

    @Test
    public void test_promoteAPMemberToCPMember() throws ExecutionException, InterruptedException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance4 = Hazelcast.newHazelcastInstance(config);

        waitUntilCPDiscoveryCompleted(instance1, instance2, instance3, instance4);

        assertTrueEventually(() -> {
            ConnectionResponse response = new HTTPCommunicator(instance4).promoteCPMember(clusterName, null);
            assertEquals(200, response.responseCode);
        });

        Collection<CPMember> cpMembers = instance1.getCPSubsystem().getCPSubsystemManagementService().getCPMembers()
                                                  .toCompletableFuture().get();
        assertEquals(4, cpMembers.size());
        assertNotNull(instance4.getCPSubsystem().getLocalCPMember());
    }

    @Test
    public void test_promoteAPMemberToCPMember_withInvalidCredentials() throws IOException, ExecutionException, InterruptedException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance4 = Hazelcast.newHazelcastInstance(config);

        ConnectionResponse response = new HTTPCommunicator(instance4).promoteCPMember("x", "x");

        assertEquals(403, response.responseCode);

        Collection<CPMember> cpMembers = instance1.getCPSubsystem().getCPSubsystemManagementService().getCPMembers()
                                                  .toCompletableFuture().get();
        assertEquals(3, cpMembers.size());
    }

    @Test
    public void test_promoteExistingCPMember() throws IOException, ExecutionException, InterruptedException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        waitUntilCPDiscoveryCompleted(instance1);

        ConnectionResponse response = new HTTPCommunicator(instance1).promoteCPMember(clusterName, null);

        assertEquals(200, response.responseCode);

        Collection<CPMember> cpMembers = instance1.getCPSubsystem().getCPSubsystemManagementService().getCPMembers()
                                                  .toCompletableFuture().get();
        assertEquals(3, cpMembers.size());
    }

    @Test
    public void test_reset() throws ExecutionException, InterruptedException, IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        instance1.getCPSubsystem().getAtomicLong("long1").set(5);

        CPGroup cpGroup1 = instance1.getCPSubsystem()
                                    .getCPSubsystemManagementService()
                                    .getCPGroup(DEFAULT_GROUP_NAME)
                                    .toCompletableFuture()
                                    .get();

        sleepAtLeastMillis(10);

        ConnectionResponse response = new HTTPCommunicator(instance1).restart(clusterName, null);
        assertEquals(200, response.responseCode);

        instance1.getCPSubsystem().getAtomicLong("long1").set(5);

        CPGroup cpGroup2 = instance1.getCPSubsystem()
                                    .getCPSubsystemManagementService()
                                    .getCPGroup(DEFAULT_GROUP_NAME)
                                    .toCompletableFuture()
                                    .get();

        RaftGroupId id1 = (RaftGroupId) cpGroup1.id();
        RaftGroupId id2 = (RaftGroupId) cpGroup2.id();

        assertTrue(id2.getSeed() > id1.getSeed());
    }

    @Test
    public void test_reset_withInvalidCredentials() throws IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        for (HazelcastInstance instance : Arrays.asList(instance1, instance2, instance3)) {
            ConnectionResponse response = new HTTPCommunicator(instance).restart("x", "x");

            assertEquals(403, response.responseCode);
        }
    }

    @Test
    public void test_getCPSessions() throws IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        instance1.getCPSubsystem().getLock("lock1").lock();
        instance1.getCPSubsystem().getLock("lock1").unlock();
        instance2.getCPSubsystem().getLock("lock1").lock();
        instance2.getCPSubsystem().getLock("lock1").unlock();
        instance3.getCPSubsystem().getLock("lock1").lock();
        instance3.getCPSubsystem().getLock("lock1").unlock();

        ConnectionResponse response = new HTTPCommunicator(instance1).getCPSessions(DEFAULT_GROUP_NAME);

        assertEquals(200, response.responseCode);

        JsonArray sessions = (JsonArray) Json.parse(response.response);

        assertEquals(3, sessions.size());
    }

    @Test
    public void test_forceCloseValidCPSession() throws IOException, ExecutionException, InterruptedException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        instance1.getCPSubsystem().getLock("lock1").lock();
        instance1.getCPSubsystem().getLock("lock1").unlock();

        Collection<CPSession> sessions1 = instance1.getCPSubsystem()
                                                  .getCPSessionManagementService()
                                                  .getAllSessions(DEFAULT_GROUP_NAME)
                                                   .toCompletableFuture()
                                                   .get();

        assertEquals(1, sessions1.size());

        long sessionId = sessions1.iterator().next().id();

        ConnectionResponse response = new HTTPCommunicator(instance1).forceCloseCPSession(DEFAULT_GROUP_NAME, sessionId, clusterName, null);

        assertEquals(200, response.responseCode);

        Collection<CPSession> sessions2 = instance1.getCPSubsystem()
                                                   .getCPSessionManagementService()
                                                   .getAllSessions(DEFAULT_GROUP_NAME)
                                                   .toCompletableFuture()
                                                   .get();

        assertEquals(0, sessions2.size());
    }

    @Test
    public void test_forceCloseValidCPSession_withInvalidCredentials() throws IOException, ExecutionException, InterruptedException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        instance1.getCPSubsystem().getLock("lock1").lock();
        instance1.getCPSubsystem().getLock("lock1").unlock();

        Collection<CPSession> sessions1 = instance1.getCPSubsystem()
                                                   .getCPSessionManagementService()
                                                   .getAllSessions(DEFAULT_GROUP_NAME)
                                                   .toCompletableFuture()
                                                   .get();

        assertEquals(1, sessions1.size());

        long sessionId = sessions1.iterator().next().id();

        ConnectionResponse response = new HTTPCommunicator(instance1).forceCloseCPSession(DEFAULT_GROUP_NAME, sessionId, "x", "x");

        assertEquals(403, response.responseCode);

        Collection<CPSession> sessions2 = instance1.getCPSubsystem()
                                                   .getCPSessionManagementService()
                                                   .getAllSessions(DEFAULT_GROUP_NAME)
                                                   .toCompletableFuture()
                                                   .get();

        assertEquals(1, sessions2.size());
    }

    @Test
    public void test_forceCloseInvalidCPSession() throws IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        instance1.getCPSubsystem().getAtomicLong("long1").set(5);

        ConnectionResponse response1 = new HTTPCommunicator(instance1).forceCloseCPSession(DEFAULT_GROUP_NAME, 1, clusterName, null);

        assertEquals(400, response1.responseCode);
    }

    @Test
    public void test_forceCloseCPSessionOfInvalidCPGroup() throws IOException {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        ConnectionResponse response1 = new HTTPCommunicator(instance1).forceCloseCPSession(DEFAULT_GROUP_NAME, 1, clusterName, null);

        assertEquals(400, response1.responseCode);
    }

}
