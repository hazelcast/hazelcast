/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.type;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;

import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.jet.sql.SqlTestSupport.assertRowsAnyOrder;
import static com.hazelcast.jet.sql.SqlTestSupport.setupCompactTypesForNestedQuery;
import static com.hazelcast.jet.sql.SqlTestSupport.rows;
import static com.hazelcast.jet.sql.SqlTestSupport.setupPortableTypesForNestedQuery;
import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static com.hazelcast.jet.sql.impl.type.SqlLazyDeserializationTestUtil.ResultCaptor;
import static com.hazelcast.jet.sql.impl.type.SqlLazyDeserializationTestUtil.SqlLazyDeserializationTestInstanceFactory;

/**
 * This test tests if nested field access for compact and portable uses lazy deserialization or not.
 * This test does not extend SqlTestSupport on purpose because we want full control over the member
 * we start.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlLazyDeserializationTest {

    private final SqlLazyDeserializationTestInstanceFactory mockInstanceFactory
            = new SqlLazyDeserializationTestInstanceFactory();

    private HazelcastInstance instance;
    private HazelcastInstance client;

    private InternalSerializationService serializationService;

    @Before
    public void before() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        config.setProperty(SQL_CUSTOM_TYPES_ENABLED.getName(), "true");

        instance = mockInstanceFactory.newHazelcastInstance(config);
        serializationService = Util.getHazelcastInstanceImpl(instance).getSerializationService();

        ClassDefinition officeType = new ClassDefinitionBuilder(1, 3)
                .addLongField("id")
                .addStringField("name")
                .build();
        serializationService.getPortableContext().registerClassDefinition(officeType);

        ClassDefinition organizationType = new ClassDefinitionBuilder(1, 2)
                .addLongField("id")
                .addStringField("name")
                .addPortableField("office", officeType)
                .build();
        serializationService.getPortableContext().registerClassDefinition(organizationType);

        ClassDefinition userType = new ClassDefinitionBuilder(1, 1)
                .addLongField("id")
                .addStringField("name")
                .addPortableField("organization", organizationType)
                .build();

        serializationService.getPortableContext().registerClassDefinition(userType);

        client = mockInstanceFactory.newHazelcastClient();
    }

    @After
    public void after() {
        mockInstanceFactory.terminateAll();
    }

    @Test
    public void test_compactNestedQueryLazyDeserialization() throws IOException {
        setupCompactTypesForNestedQuery(client);
        ResultCaptor resultCaptor = new ResultCaptor();
        doAnswer(resultCaptor).when(serializationService).readAsInternalGenericRecord(any(Data.class));

        client.getSql().execute("INSERT INTO test VALUES (1, 1, 'user1', (1, 'organization1', (1, 'office1')))");
        assertRowsAnyOrder(instance, "SELECT (organization).office.name FROM test", rows(1, "office1"));

        // 1. ColumnExpression uses readAsInternalGenericRecord to get the user type.
        verify(serializationService, times(1)).readAsInternalGenericRecord(any(Data.class));
        // There should be three InternalGenericRecords in results:
        // 1. UserType that is accessed using readAsInternalGenericRecord
        // 2,3. Organization and office types that are accessed using getInternalGenericRecord.
        CopyOnWriteArrayList<InternalGenericRecord> results = resultCaptor.getResults();
        assertEquals(results.size(), 3);
        // 2. FieldAccessExpression uses getInternalGenericRecord to get the organization
        verify(results.get(0), times(1)).getInternalGenericRecord("organization");
        // 3. FieldAccessExpression uses getInternalGenericRecord to get office
        verify(results.get(1), times(1)).getInternalGenericRecord("office");
    }

    @Test
    public void test_portableNestedQueryLazyDeserialization() throws IOException {
        setupPortableTypesForNestedQuery(client);
        ResultCaptor resultCaptor = new ResultCaptor();
        doAnswer(resultCaptor).when(serializationService).readAsInternalGenericRecord(any(Data.class));

        client.getSql().execute("INSERT INTO test VALUES (1, 1, 'user1', (1, 'organization1', (1, 'office1')))");
        assertRowsAnyOrder(instance, "SELECT (organization).office.name FROM test", rows(1, "office1"));

        // 1. ColumnExpression uses readAsInternalGenericRecord to get the user type.
        verify(serializationService, times(1)).readAsInternalGenericRecord(any(Data.class));
        CopyOnWriteArrayList<InternalGenericRecord> results = resultCaptor.getResults();
        // There should be three InternalGenericRecords in results:
        // 1. UserType that is accessed using readAsInternalGenericRecord
        // 2,3. Organization and office types that are accessed using getInternalGenericRecord.
        assertEquals(results.size(), 3);
        // 2. FieldAccessExpression uses getInternalGenericRecord to get the organization
        verify(results.get(0), times(1)).getInternalGenericRecord("organization");
        // 3. FieldAccessExpression uses getInternalGenericRecord to get office
        verify(results.get(1), times(1)).getInternalGenericRecord("office");
    }
}
