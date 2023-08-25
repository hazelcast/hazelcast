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

package com.hazelcast.dataconnection.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.impl.DataConnectionTestUtil.DummyDataConnection;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.dataconnection.impl.HikariTestUtil.assertEventuallyNoHikariThreads;
import static com.hazelcast.jet.core.TestUtil.createMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataConnectionServiceImplTest extends HazelcastTestSupport {

    private static final String TEST_CONFIG = "test-config";
    private static final String TEST_DYNAMIC_CONFIG = "test-dynamic-config";
    private static final String TEST_VIA_SERVICE_CONFIG = "test-via-service-config";
    public static final String DUMMY_DATA_CONNECTION_TYPE = "DUMMY";

    private final Config config = smallInstanceConfig();
    private final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(1);
    private HazelcastInstance instance;
    private InternalDataConnectionService dataConnectionService;

    @Before
    public void configure() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig()
                .setName(TEST_CONFIG)
                .setType(DUMMY_DATA_CONNECTION_TYPE)
                .setProperty("customProperty", "value");

        config.addDataConnectionConfig(dataConnectionConfig);
        dataConnectionService = getDataConnectionService();
    }

    @After
    public void tearDown() throws Exception {
        hazelcastInstanceFactory.shutdownAll();
        assertEventuallyNoHikariThreads(TEST_CONFIG);
    }

    @Test
    public void should_fail_when_non_existing_data_connection() {
        assertThatThrownBy(() -> dataConnectionService.getAndRetainDataConnection("non-existing-data-connection", DummyDataConnection.class))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data connection 'non-existing-data-connection' not found");
    }

    @Test
    public void should_return_data_connection_from_config() {
        DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(TEST_CONFIG, DummyDataConnection.class);

        assertThat(dataConnection).isInstanceOf(DummyDataConnection.class);
        assertThat(dataConnection.getName()).isEqualTo(TEST_CONFIG);
        assertThat(dataConnection.getConfig().getProperties())
                .containsEntry("customProperty", "value");
    }

    @Test
    public void should_return_dynamically_added_data_connection() {
        instance.getConfig().addDataConnectionConfig(
                new DataConnectionConfig(TEST_DYNAMIC_CONFIG)
                        .setType(DUMMY_DATA_CONNECTION_TYPE)
                        .setProperty("customProperty", "value")
        );

        DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(TEST_DYNAMIC_CONFIG, DummyDataConnection.class);

        assertThat(dataConnection).isInstanceOf(DummyDataConnection.class);
        assertThat(dataConnection.getName()).isEqualTo(TEST_DYNAMIC_CONFIG);
        assertThat(dataConnection.getConfig().getProperties())
                .containsEntry("customProperty", "value");
    }

    @Test
    public void should_replace_sql_data_connection() {
        dataConnectionService.createOrReplaceSqlDataConnection(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_CONNECTION_TYPE,
                true,
                createMap("customProperty", "value")
        );

        DummyDataConnection dataConnectionViaService = dataConnectionService.getAndRetainDataConnection(TEST_VIA_SERVICE_CONFIG, DummyDataConnection.class);
        dataConnectionViaService.release();

        instance.getConfig().addDataConnectionConfig(
                new DataConnectionConfig(TEST_VIA_SERVICE_CONFIG)
                        .setType(DUMMY_DATA_CONNECTION_TYPE)
                        .setProperty("customProperty", "new value")
        );

        assertTrueEventually(() -> assertThat(dataConnectionViaService.isClosed())
                .describedAs("DataConnection should have been closed when replaced by DataConnection from dynamic config")
                .isTrue());

        DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(TEST_VIA_SERVICE_CONFIG, DummyDataConnection.class);

        assertThat(dataConnection).isInstanceOf(DummyDataConnection.class);
        assertThat(dataConnection.getName()).isEqualTo(TEST_VIA_SERVICE_CONFIG);
        assertThat(dataConnection.getConfig().getProperties())
                .containsEntry("customProperty", "new value");
    }

    @Test
    public void should_replace_shared_sql_data_connection() {
        String name = randomName();
        String name2 = randomName();

        dataConnectionService.createOrReplaceSqlDataConnection(
                name,
                DUMMY_DATA_CONNECTION_TYPE,
                false,
                createMap("customProperty", "value")
        );

        dataConnectionService.createOrReplaceSqlDataConnection(
                name2,
                DUMMY_DATA_CONNECTION_TYPE,
                true,
                createMap("customProperty", "value")
        );

        DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(name, DummyDataConnection.class);
        assertThat(dataConnection.getName()).isEqualTo(name);
        assertThat(dataConnection.getConfig().isShared()).isFalse();

        DataConnection dataConnection2 = dataConnectionService.getAndRetainDataConnection(name2, DummyDataConnection.class);
        assertThat(dataConnection2.getName()).isEqualTo(name2);
        assertThat(dataConnection2.getConfig().isShared()).isTrue();
    }

    @Test
    public void new_data_connection_is_returned_after_replace() {
        dataConnectionService.createOrReplaceSqlDataConnection(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_CONNECTION_TYPE,
                false,
                createMap("customProperty", "value")
        );

        DummyDataConnection oldDataConnection = dataConnectionService.getAndRetainDataConnection(TEST_VIA_SERVICE_CONFIG, DummyDataConnection.class);

        dataConnectionService.createOrReplaceSqlDataConnection(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_CONNECTION_TYPE,
                false,
                createMap("customProperty", "new value")
        );
        DummyDataConnection newDataConnection = dataConnectionService.getAndRetainDataConnection(TEST_VIA_SERVICE_CONFIG, DummyDataConnection.class);

        assertThat(newDataConnection).isNotSameAs(oldDataConnection);
        assertThat(newDataConnection.getConfig().getProperties())
                .containsEntry("customProperty", "new value");
    }

    @Test
    public void replace_should_close_old_data_connection() {
        dataConnectionService.createOrReplaceSqlDataConnection(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_CONNECTION_TYPE,
                false,
                createMap("customProperty", "value")
        );

        DummyDataConnection dataConnectionViaService = dataConnectionService.getAndRetainDataConnection(TEST_VIA_SERVICE_CONFIG, DummyDataConnection.class);
        dataConnectionViaService.release();

        dataConnectionService.createOrReplaceSqlDataConnection(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_CONNECTION_TYPE,
                false,
                createMap("customProperty", "value2")
        );

        assertTrueEventually(() -> assertThat(dataConnectionViaService.isClosed())
                .describedAs("DataConnection should have been closed when replaced")
                .isTrue());
    }

    @Test
    public void should_return_config_created_via_service_with_config() {
        dataConnectionService.createConfigDataConnection(
                new DataConnectionConfig(TEST_VIA_SERVICE_CONFIG)
                        .setType(DUMMY_DATA_CONNECTION_TYPE)
                        .setProperty("customProperty", "value")
        );

        DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(TEST_VIA_SERVICE_CONFIG, DummyDataConnection.class);

        assertThat(dataConnection).isInstanceOf(DummyDataConnection.class);
        assertThat(dataConnection.getName()).isEqualTo(TEST_VIA_SERVICE_CONFIG);
        assertThat(dataConnection.getConfig().getProperties())
                .containsEntry("customProperty", "value");
    }

    @Test
    public void should_return_config_created_via_service_with_options() {
        dataConnectionService.createOrReplaceSqlDataConnection(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_CONNECTION_TYPE,
                false,
                createMap("customProperty", "value")
        );

        DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(TEST_VIA_SERVICE_CONFIG, DummyDataConnection.class);

        assertThat(dataConnection).isInstanceOf(DummyDataConnection.class);
        assertThat(dataConnection.getName()).isEqualTo(TEST_VIA_SERVICE_CONFIG);
        assertThat(dataConnection.getConfig().getProperties())
                .containsEntry("customProperty", "value");
    }

    @Test
    public void create_via_service_should_fail_when_dynamic_config_exists() {
        instance.getConfig().addDataConnectionConfig(
                new DataConnectionConfig(TEST_DYNAMIC_CONFIG)
                        .setType(DUMMY_DATA_CONNECTION_TYPE)
                        .setProperty("customProperty", "value")
        );

        assertThatThrownBy(() -> dataConnectionService.createOrReplaceSqlDataConnection(
                TEST_DYNAMIC_CONFIG, // same name as in config added above
                DUMMY_DATA_CONNECTION_TYPE,
                false,
                createMap("customProperty", "value")
        )).isInstanceOf(HazelcastException.class)
                .hasMessage("Cannot replace a data connection created from configuration");
    }

    @Test
    public void given_NON_existing_data_connection_type_then_fail() {
        Config wrongConfig = smallInstanceConfig();

        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig()
                .setName("data-connection-name")
                .setType("non-existing-type-name");
        wrongConfig.addDataConnectionConfig(dataConnectionConfig);

        assertThatThrownBy(() -> hazelcastInstanceFactory.newHazelcastInstance(wrongConfig))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data connection 'data-connection-name' misconfigured: "
                        + "unknown type 'non-existing-type-name'");
    }

    @Test
    public void given_data_connection_when_remove_then_data_connection_is_removed() {
        dataConnectionService.createOrReplaceSqlDataConnection(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_CONNECTION_TYPE,
                false,
                createMap("customProperty", "value")
        );

        dataConnectionService.removeDataConnection(TEST_VIA_SERVICE_CONFIG);

        assertThatThrownBy(() -> dataConnectionService.getAndRetainDataConnection(TEST_VIA_SERVICE_CONFIG, DummyDataConnection.class))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data connection 'test-via-service-config' not found");
    }

    @Test
    public void given_data_connection_when_remove_then_data_connection_is_closed() {
        dataConnectionService.createOrReplaceSqlDataConnection(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_CONNECTION_TYPE,
                false,
                createMap("customProperty", "value")
        );

        DummyDataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(TEST_VIA_SERVICE_CONFIG, DummyDataConnection.class);
        dataConnection.release();

        dataConnectionService.removeDataConnection(TEST_VIA_SERVICE_CONFIG);

        assertThat(dataConnection.isClosed())
                .describedAs("DataConnection should have been closed")
                .isTrue();
    }

    @Test
    public void remove_non_existing_data_connection_should_be_no_op() {
        dataConnectionService.removeDataConnection("does-not-exist");
    }

    @Test
    public void should_throw_when_data_connection_does_not_exist() {
        assertThatThrownBy(() -> dataConnectionService.getAndRetainDataConnection("does-not-exist", JdbcDataConnection.class))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data connection 'does-not-exist' not found");
    }

    @Test
    public void should_return_true_when_config_data_connection_exists() {
        DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(TEST_CONFIG, DataConnection.class);
        assertThat(dataConnection)
                .describedAs("DataConnection created via config should exist")
                .isNotNull();
    }

    @Test
    public void should_return_true_when_sql_data_connection_exists() {
        dataConnectionService.createOrReplaceSqlDataConnection(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_CONNECTION_TYPE,
                false,
                createMap("customProperty", "value")
        );

        DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(TEST_CONFIG, DataConnection.class);
        assertThat(dataConnection)
                .describedAs("DataConnection created via service should exist")
                .isNotNull();
    }

    @Test
    public void data_connection_type_is_not_case_sensitive() {
        dataConnectionService.createOrReplaceSqlDataConnection(
                TEST_VIA_SERVICE_CONFIG,
                "DuMMy",
                false,
                createMap("customProperty", "value")
        );

        DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(TEST_CONFIG, DataConnection.class);
        assertThat(dataConnection)
                .describedAs("DataConnection with different case should be created")
                .isNotNull();
    }

    @Test
    public void exists_should_return_false_when_data_connection_removed() {
        dataConnectionService.createOrReplaceSqlDataConnection(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_CONNECTION_TYPE,
                false,
                createMap("customProperty", "value")
        );

        dataConnectionService.removeDataConnection(TEST_VIA_SERVICE_CONFIG);

        assertThatThrownBy(() -> dataConnectionService.getAndRetainDataConnection(TEST_VIA_SERVICE_CONFIG, DataConnection.class))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data connection '" + TEST_VIA_SERVICE_CONFIG + "' not found");
    }

    @Test
    public void given_data_connection_in_config_when_remove_then_fail() {
        assertThatThrownBy(() -> dataConnectionService.removeDataConnection(TEST_CONFIG))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data connection 'test-config' is configured via Config and can't be removed");
    }

    @Test
    public void given_data_connection_when_shutdown_service_then_should_close_data_connections() {
        DummyDataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(TEST_CONFIG, DummyDataConnection.class);

        dataConnectionService.shutdown();

        assertThat(dataConnection.isClosed())
                .describedAs("DataConnection should have been closed")
                .isTrue();
    }

    @Test
    public void should_fail_when_non_existing_dataConnection() {
        assertThatThrownBy(() -> dataConnectionService.getAndRetainDataConnection("non-existing-data-connection", DummyDataConnection.class))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data connection 'non-existing-data-connection' not found");
    }

    @Test
    public void should_return_data_connection_type() {
        String type = dataConnectionService.typeForDataConnection(TEST_CONFIG);
        assertThat(type).isEqualTo("dummy");
    }

    @Test
    public void type_for_data_connection_should_throw_when_data_connection_does_not_exist() {
        assertThatThrownBy(() -> dataConnectionService.typeForDataConnection("non-existing-data-connection"))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data connection 'non-existing-data-connection' not found");
    }

    private InternalDataConnectionService getDataConnectionService() {
        instance = hazelcastInstanceFactory.newHazelcastInstance(config);
        return Util.getNodeEngine(instance).getDataConnectionService();
    }
}
