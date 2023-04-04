/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.codec.SqlMappingDdlCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.config.Config;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.PartitionAware;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;

public class MCMessageTasksTest extends SqlTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(2, createConfig(smallInstanceConfig()), null);
    }

    @Before
    public void setup() {
        warmUpPartitions(instances());
        // ensure that client knows owners of all partitions before sending message.
        // if the partition owner is not known, message would not be sent.
        warmUpPartitions(client());
    }


    protected static Config createConfig(Config baseConfig) {
        // disable backups so tests that want to ensure that there is no data in given member are easier
        baseConfig.getMapConfig("default").setBackupCount(0);
        return baseConfig;
    }

    @Test
    public void test_sqlMappingDdl_nonExistingMap() throws Exception {
        String response = getMappingDdl(randomMapName(), null);
        assertNull(response);
    }

    @Test
    public void test_sqlMappingDdl_existingMap() throws Exception {
        String name = randomMapName();
        String key = generateKeyOwnedBy(instance());
        instance().getMap(name).put(key, "value-1");

        String response = getMappingDdl(name, key);
        assertThat(response)
                .startsWith("CREATE OR REPLACE EXTERNAL MAPPING \"hazelcast\".\"public\".\"" + name + "\"")
                .containsIgnoringWhitespaces("'keyFormat' = 'java'")
                .containsIgnoringWhitespaces("'valueFormat' = 'java'");

        instance().getSql().execute(response).close();
        assertThat(instance().getSql().execute("SELECT * FROM \"" + name + "\"")).hasSize(1);
    }

    @Test
    public void test_sqlMappingDdl_existingMapDifferentPartition() throws Exception {
        String name = randomMapName();
        String key = generateKeyOwnedBy(instance());
        instance().getMap(name).put(key, "value-1");

        String someKey = generateKeyNotOwnedBy(instance());

        String response = getMappingDdl(name, someKey);
        assertThat(response).isNull();
    }

    @Test
    public void test_sqlMappingDdl_existingMapPortableKey() throws Exception {
        String name = randomMapName();
        String key = generateKeyOwnedBy(instance());
        instance().getMap(name).put(new PortableKeyPojo(key), key);

        String response = getMappingDdl(name, key);
        assertThat(response)
                .startsWith("CREATE OR REPLACE EXTERNAL MAPPING \"hazelcast\".\"public\".\"" + name + "\"")
                .containsIgnoringWhitespaces("'keyFormat' = 'portable'");

        instance().getSql().execute(response).close();
        assertThat(instance().getSql().execute("SELECT * FROM \"" + name + "\"")).hasSize(1);
    }

    @Test
    public void test_sqlMappingDdl_existingMapPortableValue() throws Exception {
        String name = randomMapName();
        String key = generateKeyOwnedBy(instance());
        instance().getMap(name).put(key, new PortableKeyPojo(key));

        String response = getMappingDdl(name, key);
        assertThat(response)
                .startsWith("CREATE OR REPLACE EXTERNAL MAPPING \"hazelcast\".\"public\".\"" + name + "\"")
                .containsIgnoringWhitespaces("'valueFormat' = 'portable'");

        instance().getSql().execute(response).close();
        assertThat(instance().getSql().execute("SELECT * FROM \"" + name + "\"")).hasSize(1);
    }

    @Test
    public void test_sqlMappingDdl_emptyMap() throws Exception {
        String name = randomMapName();
        instance().getMap(name).clear();

        String response = getMappingDdl(name, null);
        assertNull(response);
    }

    private HazelcastClientInstanceImpl getClientImpl() {
        return ((HazelcastClientProxy) client()).client;
    }

    private String getMappingDdl(String name, String partitionKey) throws InterruptedException, ExecutionException, TimeoutException {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                SqlMappingDdlCodec.encodeRequest(name),
                null,
                // send message to specific node
                partitionKey != null ? getPartitionId(instance(), partitionKey) : -1
        );

        ClientDelegatingFuture<String> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                SqlMappingDdlCodec::decodeResponse
        );

        return future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    private static final int PORTABLE_FACTORY_ID = 1;
    private static final int PORTABLE_KEY_CLASS_ID = 2;
    private static class PortableKeyPojo implements Portable, PartitionAware<String> {
        private String key;

        private PortableKeyPojo(String value) {
            this.key = value;
        }

        @Override
        public int getFactoryId() {
            return PORTABLE_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return PORTABLE_KEY_CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeString("key_p", key);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            key = reader.readString("key_p");
        }

        @Override
        public String getPartitionKey() {
            return key;
        }
    }
}
