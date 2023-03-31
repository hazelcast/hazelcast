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

package com.hazelcast.datalink.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.datalink.DataLink;
import com.hazelcast.datalink.impl.DataLinkTestUtil.DummyDataLink;
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

import static com.hazelcast.datalink.impl.HikariTestUtil.assertEventuallyNoHikariThreads;
import static com.hazelcast.jet.core.TestUtil.createMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataLinkServiceImplTest extends HazelcastTestSupport {

    private static final String TEST_CONFIG = "test-config";
    private static final String TEST_DYNAMIC_CONFIG = "test-dynamic-config";
    private static final String TEST_VIA_SERVICE_CONFIG = "test-via-service-config";
    public static final String DUMMY_DATA_LINK_TYPE = "DUMMY";

    private final Config config = smallInstanceConfig();
    private final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(1);
    private HazelcastInstance instance;
    private InternalDataLinkService dataLinkService;

    @Before
    public void configure() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig()
                .setName(TEST_CONFIG)
                .setType(DUMMY_DATA_LINK_TYPE)
                .setProperty("customProperty", "value");

        config.addDataLinkConfig(dataLinkConfig);
        dataLinkService = getDataLinkService();
    }

    @After
    public void tearDown() throws Exception {
        hazelcastInstanceFactory.shutdownAll();
        assertEventuallyNoHikariThreads(TEST_CONFIG);
    }

    @Test
    public void should_fail_when_non_existing_data_link() {
        assertThatThrownBy(() -> dataLinkService.getAndRetainDataLink("non-existing-data-link", DummyDataLink.class))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data link 'non-existing-data-link' not found");
    }

    @Test
    public void should_return_data_link_from_config() {
        DataLink dataLink = dataLinkService.getAndRetainDataLink(TEST_CONFIG, DummyDataLink.class);

        assertThat(dataLink).isInstanceOf(DummyDataLink.class);
        assertThat(dataLink.getName()).isEqualTo(TEST_CONFIG);
        assertThat(dataLink.getConfig().getProperties())
                .containsEntry("customProperty", "value");
    }

    @Test
    public void should_return_dynamically_added_data_link() {
        instance.getConfig().addDataLinkConfig(
                new DataLinkConfig(TEST_DYNAMIC_CONFIG)
                        .setType(DUMMY_DATA_LINK_TYPE)
                        .setProperty("customProperty", "value")
        );

        DataLink dataLink = dataLinkService.getAndRetainDataLink(TEST_DYNAMIC_CONFIG, DummyDataLink.class);

        assertThat(dataLink).isInstanceOf(DummyDataLink.class);
        assertThat(dataLink.getName()).isEqualTo(TEST_DYNAMIC_CONFIG);
        assertThat(dataLink.getConfig().getProperties())
                .containsEntry("customProperty", "value");
    }

    @Test
    public void should_replace_sql_data_link() {
        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_LINK_TYPE,
                true,
                createMap("customProperty", "value")
        );

        DummyDataLink dataLinkViaService = dataLinkService.getAndRetainDataLink(TEST_VIA_SERVICE_CONFIG, DummyDataLink.class);
        dataLinkViaService.release();

        instance.getConfig().addDataLinkConfig(
                new DataLinkConfig(TEST_VIA_SERVICE_CONFIG)
                        .setType(DUMMY_DATA_LINK_TYPE)
                        .setProperty("customProperty", "new value")
        );

        assertTrueEventually(() -> assertThat(dataLinkViaService.isClosed())
                    .describedAs("DataLink should have been closed when replaced by DataLink from dynamic config")
                    .isTrue());

        DataLink dataLink = dataLinkService.getAndRetainDataLink(TEST_VIA_SERVICE_CONFIG, DummyDataLink.class);

        assertThat(dataLink).isInstanceOf(DummyDataLink.class);
        assertThat(dataLink.getName()).isEqualTo(TEST_VIA_SERVICE_CONFIG);
        assertThat(dataLink.getConfig().getProperties())
                .containsEntry("customProperty", "new value");
    }

    @Test
    public void should_replace_shared_sql_data_link() {
        String name = randomName();
        String name2 = randomName();

        dataLinkService.replaceSqlDataLink(
                name,
                DUMMY_DATA_LINK_TYPE,
                false,
                createMap("customProperty", "value")
        );

        dataLinkService.replaceSqlDataLink(
                name2,
                DUMMY_DATA_LINK_TYPE,
                true,
                createMap("customProperty", "value")
        );

        DataLink dataLink = dataLinkService.getAndRetainDataLink(name, DummyDataLink.class);
        assertThat(dataLink.getName()).isEqualTo(name);
        assertThat(dataLink.getConfig().isShared()).isFalse();

        DataLink dataLink2 = dataLinkService.getAndRetainDataLink(name2, DummyDataLink.class);
        assertThat(dataLink2.getName()).isEqualTo(name2);
        assertThat(dataLink2.getConfig().isShared()).isTrue();
    }

    @Test
    public void new_data_link_is_returned_after_replace() {
        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_LINK_TYPE,
                false,
                createMap("customProperty", "value")
        );

        DummyDataLink oldDataLink = dataLinkService.getAndRetainDataLink(TEST_VIA_SERVICE_CONFIG, DummyDataLink.class);

        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_LINK_TYPE,
                false,
                createMap("customProperty", "new value")
        );
        DummyDataLink newDataLink = dataLinkService.getAndRetainDataLink(TEST_VIA_SERVICE_CONFIG, DummyDataLink.class);

        assertThat(newDataLink).isNotSameAs(oldDataLink);
        assertThat(newDataLink.getConfig().getProperties())
                .containsEntry("customProperty", "new value");
    }

    @Test
    public void replace_should_close_old_data_link() {
        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_LINK_TYPE,
                false,
                createMap("customProperty", "value")
        );

        DummyDataLink dataLinkViaService = dataLinkService.getAndRetainDataLink(TEST_VIA_SERVICE_CONFIG, DummyDataLink.class);
        dataLinkViaService.release();

        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_LINK_TYPE,
                false,
                createMap("customProperty", "value2")
        );

        assertTrueEventually(() -> assertThat(dataLinkViaService.isClosed())
                .describedAs("DataLink should have been closed when replaced")
                .isTrue());
    }

    @Test
    public void should_return_config_created_via_service_with_config() {
        dataLinkService.createConfigDataLink(
                new DataLinkConfig(TEST_VIA_SERVICE_CONFIG)
                        .setType(DUMMY_DATA_LINK_TYPE)
                        .setProperty("customProperty", "value")
        );

        DataLink dataLink = dataLinkService.getAndRetainDataLink(TEST_VIA_SERVICE_CONFIG, DummyDataLink.class);

        assertThat(dataLink).isInstanceOf(DummyDataLink.class);
        assertThat(dataLink.getName()).isEqualTo(TEST_VIA_SERVICE_CONFIG);
        assertThat(dataLink.getConfig().getProperties())
                .containsEntry("customProperty", "value");
    }

    @Test
    public void should_return_config_created_via_service_with_options() {
        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_LINK_TYPE,
                false,
                createMap("customProperty", "value")
        );

        DataLink dataLink = dataLinkService.getAndRetainDataLink(TEST_VIA_SERVICE_CONFIG, DummyDataLink.class);

        assertThat(dataLink).isInstanceOf(DummyDataLink.class);
        assertThat(dataLink.getName()).isEqualTo(TEST_VIA_SERVICE_CONFIG);
        assertThat(dataLink.getConfig().getProperties())
                .containsEntry("customProperty", "value");
    }

    @Test
    public void create_via_service_should_fail_when_dynamic_config_exists() {
        instance.getConfig().addDataLinkConfig(
                new DataLinkConfig(TEST_DYNAMIC_CONFIG)
                        .setType(DUMMY_DATA_LINK_TYPE)
                        .setProperty("customProperty", "value")
        );

        assertThatThrownBy(() -> dataLinkService.replaceSqlDataLink(
                TEST_DYNAMIC_CONFIG, // same name as in config added above
                DUMMY_DATA_LINK_TYPE,
                false,
                createMap("customProperty", "value")
        )).isInstanceOf(HazelcastException.class)
          .hasMessage("Cannot replace a data link created from configuration");
    }

    @Test
    public void given_NON_existing_data_link_type_then_fail() {
        Config wrongConfig = smallInstanceConfig();

        DataLinkConfig dataLinkConfig = new DataLinkConfig()
                .setName("data-link-name")
                .setType("non-existing-type-name");
        wrongConfig.addDataLinkConfig(dataLinkConfig);

        assertThatThrownBy(() -> hazelcastInstanceFactory.newHazelcastInstance(wrongConfig))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data link 'data-link-name' misconfigured: "
                        + "unknown type 'non-existing-type-name'");
    }

    @Test
    public void given_data_link_when_remove_then_data_link_is_removed() {
        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_LINK_TYPE,
                false,
                createMap("customProperty", "value")
        );

        dataLinkService.removeDataLink(TEST_VIA_SERVICE_CONFIG);

        assertThatThrownBy(() -> dataLinkService.getAndRetainDataLink(TEST_VIA_SERVICE_CONFIG, DummyDataLink.class))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data link 'test-via-service-config' not found");
    }

    @Test
    public void given_data_link_when_remove_then_data_link_is_closed() {
        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_LINK_TYPE,
                false,
                createMap("customProperty", "value")
        );

        DummyDataLink dataLink = dataLinkService.getAndRetainDataLink(TEST_VIA_SERVICE_CONFIG, DummyDataLink.class);
        dataLink.release();

        dataLinkService.removeDataLink(TEST_VIA_SERVICE_CONFIG);

        assertThat(dataLink.isClosed())
                .describedAs("DataLink should have been closed")
                .isTrue();
    }

    @Test
    public void remove_non_existing_data_link_should_be_no_op() {
        dataLinkService.removeDataLink("does-not-exist");
    }

    @Test
    public void should_throw_when_data_link_does_not_exist() {
        assertThatThrownBy(() -> dataLinkService.getAndRetainDataLink("does-not-exist", JdbcDataLink.class))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data link 'does-not-exist' not found");
    }

    @Test
    public void should_return_true_when_config_data_link_exists() {
        DataLink dataLink = dataLinkService.getAndRetainDataLink(TEST_CONFIG, DataLink.class);
        assertThat(dataLink)
                .describedAs("DataLink created via config should exist")
                .isNotNull();
    }

    @Test
    public void should_return_true_when_sql_data_link_exists() {
        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_LINK_TYPE,
                false,
                createMap("customProperty", "value")
        );

        DataLink dataLink = dataLinkService.getAndRetainDataLink(TEST_CONFIG, DataLink.class);
        assertThat(dataLink)
                .describedAs("DataLink created via service should exist")
                .isNotNull();
    }

    @Test
    public void data_link_type_is_not_case_sensitive() {
        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG,
                "DuMMy",
                false,
                createMap("customProperty", "value")
        );

        DataLink dataLink = dataLinkService.getAndRetainDataLink(TEST_CONFIG, DataLink.class);
        assertThat(dataLink)
                .describedAs("DataLink with different case should be created")
                .isNotNull();
    }

    @Test
    public void exists_should_return_false_when_data_link_removed() {
        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG,
                DUMMY_DATA_LINK_TYPE,
                false,
                createMap("customProperty", "value")
        );

        dataLinkService.removeDataLink(TEST_VIA_SERVICE_CONFIG);

        assertThatThrownBy(() -> dataLinkService.getAndRetainDataLink(TEST_VIA_SERVICE_CONFIG, DataLink.class))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data link '" + TEST_VIA_SERVICE_CONFIG + "' not found");
    }

    @Test
    public void given_data_link_in_config_when_remove_then_fail() {
        assertThatThrownBy(() -> dataLinkService.removeDataLink(TEST_CONFIG))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data link 'test-config' is configured via Config and can't be removed");
    }

    @Test
    public void given_data_link_when_shutdown_service_then_should_close_data_links() {
        DummyDataLink dataLink = dataLinkService.getAndRetainDataLink(TEST_CONFIG, DummyDataLink.class);

        dataLinkService.shutdown();

        assertThat(dataLink.isClosed())
                .describedAs("DataLink should have been closed")
                .isTrue();
    }

    @Test
    public void should_fail_when_non_existing_dataLink() {
        assertThatThrownBy(() -> dataLinkService.getAndRetainDataLink("non-existing-data-link", DummyDataLink.class))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data link 'non-existing-data-link' not found");
    }

    @Test
    public void should_return_data_link_type() {
        String type = dataLinkService.typeForDataLink(TEST_CONFIG);
        assertThat(type).isEqualTo("DUMMY");
    }

    @Test
    public void type_for_data_link_should_throw_when_data_link_does_not_exist() {
        assertThatThrownBy(() -> dataLinkService.typeForDataLink("non-existing-data-link"))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("DataLink with name 'non-existing-data-link' does not exist");
    }

    private InternalDataLinkService getDataLinkService() {
        instance = hazelcastInstanceFactory.newHazelcastInstance(config);
        return Util.getNodeEngine(instance).getDataLinkService();
    }
}
