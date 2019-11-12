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

package com.hazelcast.internal.management;

import com.hazelcast.client.Client;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.ClientImpl;
import com.hazelcast.client.impl.ClientSelectors;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.management.dto.ClientBwListEntryDTO;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.management.dto.ClientBwListDTO.Mode;
import static com.hazelcast.internal.management.dto.ClientBwListEntryDTO.Type;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientBwListConfigHandlerTest extends HazelcastTestSupport {

    private ClientEngine clientEngine;
    private ClientBwListConfigHandler handler;

    @Before
    public void setUp() {
        HazelcastInstance instance = createHazelcastInstance();
        clientEngine = getNode(instance).getClientEngine();
        handler = new ClientBwListConfigHandler(clientEngine);
    }

    @Test
    public void testHandleLostConnection() {
        clientEngine.applySelector(ClientSelectors.none());

        handler.handleLostConnection();

        Client client = createClient("127.0.0.1", randomString());

        assertTrue(clientEngine.isClientAllowed(client));
    }

    @Test
    public void testHandleWhitelist() {
        JsonObject configJson = createConfig(Mode.WHITELIST,
                new ClientBwListEntryDTO(Type.IP_ADDRESS, "127.0.0.*"),
                new ClientBwListEntryDTO(Type.IP_ADDRESS, "192.168.0.1"),
                new ClientBwListEntryDTO(Type.IP_ADDRESS, "192.168.0.42-43"),
                new ClientBwListEntryDTO(Type.IP_ADDRESS, "fe80:0:0:0:45c5:47ee:fe15:493a"),
                new ClientBwListEntryDTO(Type.INSTANCE_NAME, "client*"),
                new ClientBwListEntryDTO(Type.LABEL, "label*"));
        handler.handleConfig(configJson);

        Client[] allowed = {
                createClient("127.0.0.3", "a_name"),
                createClient("192.168.0.1", "a_name"),
                createClient("192.168.0.42", "a_name"),
                createClient("fe80:0:0:0:45c5:47ee:fe15:493a", "a_name"),
                createClient("192.168.0.101", "client4"),
                createClient("192.168.0.101", "a_name", "label")
        };
        for (Client client : allowed) {
            assertTrue(clientEngine.isClientAllowed(client));
        }

        Client[] denied = {
                createClient("192.168.0.101", "a_name", "random"),
                createClient("fe70:0:0:0:35c5:16ee:fe15:491a", "a_name", "random")
        };
        for (Client client : denied) {
            assertFalse(clientEngine.isClientAllowed(client));
        }
    }

    @Test
    public void testHandleBlacklist() {
        JsonObject configJson = createConfig(Mode.BLACKLIST,
                new ClientBwListEntryDTO(Type.IP_ADDRESS, "127.0.0.*"),
                new ClientBwListEntryDTO(Type.IP_ADDRESS, "192.168.0.1"),
                new ClientBwListEntryDTO(Type.IP_ADDRESS, "192.168.*.42"),
                new ClientBwListEntryDTO(Type.IP_ADDRESS, "fe80:0:0:0:45c5:47ee:fe15:*"),
                new ClientBwListEntryDTO(Type.INSTANCE_NAME, "*_client"),
                new ClientBwListEntryDTO(Type.LABEL, "test*label"));
        handler.handleConfig(configJson);

        Client[] allowed = {
                createClient("192.168.0.101", "a_name", "random"),
                createClient("fe70:0:0:0:35c5:16ee:fe15:491a", "a_name", "random")
        };
        for (Client client : allowed) {
            assertTrue(clientEngine.isClientAllowed(client));
        }

        Client[] denied = {
                createClient("127.0.0.3", "a_name"),
                createClient("192.168.0.1", "a_name"),
                createClient("192.168.0.42", "a_name"),
                createClient("fe80:0:0:0:45c5:47ee:fe15:493a", "a_name"),
                createClient("192.168.0.101", "java_client"),
                createClient("192.168.0.101", "a_name", "test_label"),
                createClient("192.168.0.101", "a_name", "testlabel")
        };
        for (Client client : denied) {
            assertFalse(clientEngine.isClientAllowed(client));
        }
    }

    @Test
    public void testHandleEmptyWhitelist() {
        JsonObject configJson = createConfig(Mode.WHITELIST);
        handler.handleConfig(configJson);

        Client client = createClient("127.0.0.1", "a_name");
        assertFalse(clientEngine.isClientAllowed(client));
    }

    @Test
    public void testHandleEmptyBlacklist() {
        clientEngine.applySelector(ClientSelectors.none());

        JsonObject configJson = createConfig(Mode.BLACKLIST);
        handler.handleConfig(configJson);

        Client client = createClient("127.0.0.1", "a_name");
        assertTrue(clientEngine.isClientAllowed(client));
    }

    @Test
    public void testHandleDisabledMode() {
        clientEngine.applySelector(ClientSelectors.none());

        JsonObject configJson = createConfig(Mode.DISABLED);
        handler.handleConfig(configJson);

        Client client = createClient("127.0.0.1", randomString());
        assertTrue(clientEngine.isClientAllowed(client));
    }

    @Test
    public void testHandleEmptyConfig() {
        clientEngine.applySelector(ClientSelectors.none());

        JsonObject configJson = new JsonObject();
        handler.handleConfig(configJson);

        Client client = createClient("127.0.0.1", randomString());
        assertFalse(clientEngine.isClientAllowed(client));
    }

    @Test
    public void testHandleInvalidMode() {
        clientEngine.applySelector(ClientSelectors.none());

        JsonObject configJson = createConfig(Mode.DISABLED);
        configJson.get("clientBwList").asObject().set("mode", "invalid_mode");
        handler.handleConfig(configJson);

        Client client = createClient("127.0.0.1", randomString());
        assertFalse(clientEngine.isClientAllowed(client));
    }

    @Test
    public void testHandleInvalidEntry() {
        clientEngine.applySelector(ClientSelectors.none());

        JsonObject configJson = createConfig(Mode.WHITELIST,
                new ClientBwListEntryDTO(Type.LABEL, "192.168.0.1"),
                new ClientBwListEntryDTO(Type.IP_ADDRESS, "192.168.0.2"));
        configJson.get("clientBwList").asObject()
                .get("entries").asArray()
                .get(0).asObject().set("type", "invalid_type");
        handler.handleConfig(configJson);

        Client client1 = createClient("192.168.0.1", randomString());
        assertFalse(clientEngine.isClientAllowed(client1));

        Client client2 = createClient("192.168.0.2", randomString());
        assertFalse(clientEngine.isClientAllowed(client2));
    }

    @Test
    public void testApplyConfig() {
        clientEngine.applySelector(ClientSelectors.none());

        ClientBwListDTO config = new ClientBwListDTO(
                Mode.WHITELIST,
                singletonList(new ClientBwListEntryDTO(Type.IP_ADDRESS, "127.0.0.*"))
        );
        handler.applyConfig(config);

        Client client = createClient("127.0.0.42", randomString());
        assertTrue(clientEngine.isClientAllowed(client));
    }

    private JsonObject createConfig(Mode mode, ClientBwListEntryDTO... entries) {
        List<ClientBwListEntryDTO> entriesList = new ArrayList<ClientBwListEntryDTO>();
        if (entries != null) {
            entriesList.addAll(Arrays.asList(entries));
        }
        ClientBwListDTO configDTO = new ClientBwListDTO(mode, entriesList);
        JsonObject result = new JsonObject();
        result.add("clientBwList", configDTO.toJson());
        return result;
    }

    private Client createClient(String ip, String name, String... labels) {
        Set<String> labelsSet = new HashSet<String>();
        if (labels != null && labels.length > 0) {
            for (String label : labels) {
                labelsSet.add(label);
            }
        }
        Client client = new ClientImpl(null, createInetSocketAddress(ip), name, labelsSet);
        return client;
    }

    private InetSocketAddress createInetSocketAddress(String name) {
        try {
            return new InetSocketAddress(InetAddress.getByName(name), 5000);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }


}
