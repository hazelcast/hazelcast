/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.InnerDTOSerializer;
import example.serialization.MainDTO;
import example.serialization.MainDTOSerializer;
import example.serialization.NamedDTO;
import example.serialization.NamedDTOSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static com.hazelcast.internal.util.ThreadUtil.getThreadId;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GetCompactSchemasTest extends HazelcastTestSupport {

    protected TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance instance;
    private HazelcastInstance client;
    private String fixedSizeDTOTypeName;
    private String mainDTOTypeName;
    private Schema fixedSizeDTOSchema;
    private Schema mainDTOSchema;
    private Schema innerDTOSchema;
    private Schema namedDTOSchema;
    private FixedSizeFieldsDTOSerializer fixedSizeFieldsDTOSerializer;
    private MainDTOSerializer mainDTOSerializer;
    private InnerDTOSerializer innerDTOSerializer;
    private NamedDTOSerializer namedDTOSerializer;
    private static final String TEST_MAP_NAME = randomMapName();
    private static final String FIXED_SIZED_DTO_KEY = "fixedDTO";
    private static final String MAIN_DTO_KEY = "mainDTO";

    @Before
    public void setUp() {
        instance = factory.newHazelcastInstance(getConfig());
        FixedSizeFieldsDTO dto = CompactTestUtil.createFixedSizeFieldsDTO();
        MainDTO mainDTO = CompactTestUtil.createMainDTO();
        instance.getMap(TEST_MAP_NAME).put(FIXED_SIZED_DTO_KEY, dto);
        instance.getMap(TEST_MAP_NAME).put(MAIN_DTO_KEY, mainDTO);
        SchemaWriter schemaWriter = new SchemaWriter(fixedSizeDTOTypeName);
        fixedSizeFieldsDTOSerializer.write(schemaWriter, dto);
        fixedSizeDTOSchema = schemaWriter.build();
        SchemaWriter mainDTOSchemaWriter = new SchemaWriter(mainDTOTypeName);
        mainDTOSerializer.write(mainDTOSchemaWriter, mainDTO);
        mainDTOSchema = mainDTOSchemaWriter.build();
        SchemaWriter innerDTOSchemaWriter = new SchemaWriter(innerDTOSerializer.getTypeName());
        innerDTOSerializer.write(innerDTOSchemaWriter, CompactTestUtil.createInnerDTO());
        innerDTOSchema = innerDTOSchemaWriter.build();
        SchemaWriter namedDTOSchemaWriter = new SchemaWriter(namedDTOSerializer.getTypeName());
        namedDTOSerializer.write(namedDTOSchemaWriter, new NamedDTO());
        namedDTOSchema = namedDTOSchemaWriter.build();
        client = factory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Override
    protected Config getConfig() {
        Config c = super.getConfig();
        fixedSizeFieldsDTOSerializer = new FixedSizeFieldsDTOSerializer();
        fixedSizeDTOTypeName = fixedSizeFieldsDTOSerializer.getTypeName();
        mainDTOSerializer = new MainDTOSerializer();
        innerDTOSerializer = new InnerDTOSerializer();
        namedDTOSerializer = new NamedDTOSerializer();
        mainDTOTypeName = mainDTOSerializer.getTypeName();
        c.getSerializationConfig().getCompactSerializationConfig().addSerializer(fixedSizeFieldsDTOSerializer)
                .addSerializer(mainDTOSerializer).addSerializer(innerDTOSerializer).addSerializer(namedDTOSerializer);
        return c;
    }

    @Test
    public void testGetCompactSchemas_BasicCompact() throws ExecutionException, InterruptedException, IOException {
        assertGetCompactSchemas(FIXED_SIZED_DTO_KEY, fixedSizeDTOSchema);
    }

    @Test
    public void testGetCompactSchemas_NestedCompacts() throws ExecutionException, InterruptedException, IOException {
        assertGetCompactSchemas(MAIN_DTO_KEY, mainDTOSchema, innerDTOSchema, namedDTOSchema);
    }

    @Test
    public void testGetCompactSchemas_ThrowsIfSchemaAbsent() {
        // Clears the schemas
        getNodeEngineImpl(instance).getSchemaService().shutdown(true);
        assertThatThrownBy(() -> assertGetCompactSchemas(FIXED_SIZED_DTO_KEY, fixedSizeDTOSchema))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Schema with id")
                .hasMessageContaining("is not available in the cluster");
    }

    @Test
    public void testGetCompactSchemas_ThrowsIfInIsNull() {
        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        InternalSerializationService ss = clientInstanceImpl.getSerializationService();
        assertThatThrownBy(() -> ss.getCompactSchemas(null, new HashSet<>()))
                .isInstanceOf(IllegalStateException.class);
    }

    private void assertGetCompactSchemas(String mapKey, Schema... expected) throws ExecutionException, InterruptedException,
            IOException {
        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        Data d = getInternal(clientInstanceImpl, mapKey);
        InternalSerializationService ss = clientInstanceImpl.getSerializationService();
        BufferObjectDataInput in = ss.createObjectDataInput(d);
        Set<Schema> schemas = new HashSet<>();
        ss.getCompactSchemas(in, schemas);
        assertThat(schemas).containsExactlyInAnyOrder(expected);
    }

    private Data getInternal(HazelcastClientInstanceImpl client, Object key) throws ExecutionException,
            InterruptedException {
        Data keyData = client.getSerializationService().toData(key);
        ClientMessage request = MapGetCodec.encodeRequest(TEST_MAP_NAME, keyData, getThreadId());
        int partitionId = client.getPartitionService().getPartition(key).getPartitionId();
        ClientMessage response = new ClientInvocation(client, request, TEST_MAP_NAME, partitionId).invoke().get();
        return MapGetCodec.decodeResponse(response);
    }
}
