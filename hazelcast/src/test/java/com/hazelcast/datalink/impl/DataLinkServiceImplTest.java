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
import com.hazelcast.datalink.DataLinkService;
import com.hazelcast.datalink.JdbcDataLink;
import com.hazelcast.datalink.impl.DataLinkTestUtil.DummyDataLink;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.hazelcast.datalink.impl.HikariTestUtil.assertDataSourceClosed;
import static com.hazelcast.datalink.impl.HikariTestUtil.assertEventuallyNoHikariThreads;
import static com.hazelcast.jet.core.TestUtil.createMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataLinkServiceImplTest extends HazelcastTestSupport {

    private static final String TEST_CONFIG_NAME = "test-config";
    private static final String TEST_DYNAMIC_CONFIG_NAME = "test-dynamic-config";
    private static final String TEST_VIA_SERVICE_CONFIG_NAME = "test-via-service-config";

    private final Config config = smallInstanceConfig();
    private final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(1);
    private HazelcastInstance instance;
    private DataLinkService dataLinkService;

    @Before
    public void configure() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig()
                .setName(TEST_CONFIG_NAME)
                .setClassName(DummyDataLink.class.getName())
                .setProperty("customProperty", "value");

        config.addDataLinkConfig(dataLinkConfig);
        dataLinkService = getDataLinkService();
    }

    @After
    public void tearDown() throws Exception {
        hazelcastInstanceFactory.shutdownAll();
        assertEventuallyNoHikariThreads(TEST_CONFIG_NAME);
    }

    @Test
    public void should_fail_when_non_existing_data_link() {
        assertThatThrownBy(() -> dataLinkService.getDataLink("non-existing-data-link"))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data link 'non-existing-data-link' not found");
    }

    @Test
    public void should_return_data_link_from_config() {
        DataLink dataLink = dataLinkService.getDataLink(TEST_CONFIG_NAME);

        assertThat(dataLink).isInstanceOf(DummyDataLink.class);
        assertThat(dataLink.getName()).isEqualTo(TEST_CONFIG_NAME);
        assertThat(dataLink.getConfig().getProperties())
                .containsEntry("customProperty", "value");
    }

    @Test
    public void should_return_dynamically_added_data_link() {
        instance.getConfig().addDataLinkConfig(
                new DataLinkConfig(TEST_DYNAMIC_CONFIG_NAME)
                        .setClassName(DummyDataLink.class.getName())
                        .setProperty("customProperty", "value")
        );

        DataLink dataLink = dataLinkService.getDataLink(TEST_DYNAMIC_CONFIG_NAME);

        assertThat(dataLink).isInstanceOf(DummyDataLink.class);
        assertThat(dataLink.getName()).isEqualTo(TEST_DYNAMIC_CONFIG_NAME);
        assertThat(dataLink.getConfig().getProperties())
                .containsEntry("customProperty", "value");
    }

    @Test
    public void should_replace_sql_data_link() {
        dataLinkService.createSqlDataLink(
                TEST_VIA_SERVICE_CONFIG_NAME,
                DummyDataLink.class.getName(),
                createMap("customProperty", "value")
        );

        DummyDataLink dataLinkViaService = dataLinkService.getDataLink(TEST_VIA_SERVICE_CONFIG_NAME);

        instance.getConfig().addDataLinkConfig(
                new DataLinkConfig(TEST_VIA_SERVICE_CONFIG_NAME)
                        .setClassName(DummyDataLink.class.getName())
                        .setProperty("customProperty", "new value")
        );

        assertThat(dataLinkViaService.isClosed())
                .describedAs("DataLink should have been closed when replaced by DataLink from dynamic config")
                .isTrue();

        DataLink dataLink = dataLinkService.getDataLink(TEST_VIA_SERVICE_CONFIG_NAME);

        assertThat(dataLink).isInstanceOf(DummyDataLink.class);
        assertThat(dataLink.getName()).isEqualTo(TEST_VIA_SERVICE_CONFIG_NAME);
        assertThat(dataLink.getConfig().getProperties())
                .containsEntry("customProperty", "new value");
    }

    @Test
    public void new_data_link_is_returned_after_replace() {
        dataLinkService.createSqlDataLink(
                TEST_VIA_SERVICE_CONFIG_NAME,
                DummyDataLink.class.getName(),
                createMap("customProperty", "value")
        );

        DummyDataLink oldDataLink = dataLinkService.getDataLink(TEST_VIA_SERVICE_CONFIG_NAME);

        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG_NAME,
                DummyDataLink.class.getName(),
                createMap("customProperty", "new value")
        );
        DummyDataLink newDataLink = dataLinkService.getDataLink(TEST_VIA_SERVICE_CONFIG_NAME);

        assertThat(newDataLink).isNotSameAs(oldDataLink);
        assertThat(newDataLink.getConfig().getProperties())
                .containsEntry("customProperty", "new value");
    }

    @Test
    public void replace_should_close_old_data_link() {
        dataLinkService.createSqlDataLink(
                TEST_VIA_SERVICE_CONFIG_NAME,
                DummyDataLink.class.getName(),
                createMap("customProperty", "value")
        );

        DummyDataLink dataLinkViaService = dataLinkService.getDataLink(TEST_VIA_SERVICE_CONFIG_NAME);

        dataLinkService.replaceSqlDataLink(
                TEST_VIA_SERVICE_CONFIG_NAME,
                DummyDataLink.class.getName(),
                createMap("customProperty", "value2")
        );

        assertThat(dataLinkViaService.isClosed())
                .describedAs("DataLink should have been closed when replaced")
                .isTrue();
    }

    @Test
    public void should_return_config_created_via_service_with_config() {
        dataLinkService.createConfigDataLink(
                new DataLinkConfig(TEST_VIA_SERVICE_CONFIG_NAME)
                        .setClassName(DummyDataLink.class.getName())
                        .setProperty("customProperty", "value")
        );

        DataLink dataLink = dataLinkService.getDataLink(TEST_VIA_SERVICE_CONFIG_NAME);

        assertThat(dataLink).isInstanceOf(DummyDataLink.class);
        assertThat(dataLink.getName()).isEqualTo(TEST_VIA_SERVICE_CONFIG_NAME);
        assertThat(dataLink.getConfig().getProperties())
                .containsEntry("customProperty", "value");
    }

    @Test
    public void should_return_config_created_via_service_with_options() {
        dataLinkService.createSqlDataLink(
                TEST_VIA_SERVICE_CONFIG_NAME,
                DummyDataLink.class.getName(),
                createMap("customProperty", "value")
        );

        DataLink dataLink = dataLinkService.getDataLink(TEST_VIA_SERVICE_CONFIG_NAME);

        assertThat(dataLink).isInstanceOf(DummyDataLink.class);
        assertThat(dataLink.getName()).isEqualTo(TEST_VIA_SERVICE_CONFIG_NAME);
        assertThat(dataLink.getConfig().getProperties())
                .containsEntry("customProperty", "value");
    }

    @Test
    public void create_via_service_should_fail_when_static_config_exists() {
        assertThatThrownBy(() -> dataLinkService.createSqlDataLink(
                TEST_CONFIG_NAME, // same name as in static config
                DummyDataLink.class.getName(),
                createMap("customProperty", "value")
        )).isInstanceOf(HazelcastException.class)
          .hasMessage("Data link 'test-config' already exists");
    }

    @Test
    public void create_via_service_should_fail_when_dynamic_config_exists() {
        instance.getConfig().addDataLinkConfig(
                new DataLinkConfig(TEST_DYNAMIC_CONFIG_NAME)
                        .setClassName(DummyDataLink.class.getName())
                        .setProperty("customProperty", "value")
        );

        assertThatThrownBy(() -> dataLinkService.createSqlDataLink(
                TEST_DYNAMIC_CONFIG_NAME, // same name as in config added above
                DummyDataLink.class.getName(),
                createMap("customProperty", "value")
        )).isInstanceOf(HazelcastException.class)
          .hasMessage("Data link 'test-dynamic-config' already exists");
    }

    @Test
    public void given_NON_existing_data_link_class_then_fail() {
        Config wrongConfig = smallInstanceConfig();

        DataLinkConfig dataLinkConfig = new DataLinkConfig()
                .setName("non-existing-class")
                .setClassName("com.example.NonExistingClass");
        wrongConfig.addDataLinkConfig(dataLinkConfig);

        assertThatThrownBy(() -> hazelcastInstanceFactory.newHazelcastInstance(wrongConfig))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data link 'non-existing-class' misconfigured: "
                        + "class 'com.example.NonExistingClass' not found");
    }

    @Test
    public void given_data_link_class_DOES_NOT_implement_data_link_then_fail() {
        Config wrongConfig = smallInstanceConfig();

        DataLinkConfig dataLinkConfig = new DataLinkConfig()
                .setName("wrong-class-name")
                .setClassName(MissingImplementsDataLink.class.getName());
        wrongConfig.addDataLinkConfig(dataLinkConfig);

        assertThatThrownBy(() -> hazelcastInstanceFactory.newHazelcastInstance(wrongConfig))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data link 'wrong-class-name' misconfigured: 'com.hazelcast.datalink.impl"
                        + ".DataLinkServiceImplTest$MissingImplementsDataLink'"
                        + " must implement 'com.hazelcast.datalink.DataLink'");
    }

    @Test
    public void given_data_link_when_remove_then_data_link_is_removed() {
        dataLinkService.createSqlDataLink(
                TEST_VIA_SERVICE_CONFIG_NAME,
                DummyDataLink.class.getName(),
                createMap("customProperty", "value")
        );

        dataLinkService.removeDataLink(TEST_VIA_SERVICE_CONFIG_NAME);

        assertThatThrownBy(() -> dataLinkService.getDataLink(TEST_VIA_SERVICE_CONFIG_NAME))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data link 'test-via-service-config' not found");
    }

    @Test
    public void given_data_link_when_remove_then_data_link_is_closed() {
        dataLinkService.createSqlDataLink(
                TEST_VIA_SERVICE_CONFIG_NAME,
                DummyDataLink.class.getName(),
                createMap("customProperty", "value")
        );

        DummyDataLink dataLink = dataLinkService.getDataLink(TEST_VIA_SERVICE_CONFIG_NAME);

        dataLinkService.removeDataLink(TEST_VIA_SERVICE_CONFIG_NAME);

        assertThat(dataLink.isClosed())
                .describedAs("DataLink should have been closed")
                .isTrue();
    }

    @Test
    public void given_data_link_in_config_when_remove_then_fail() {
        assertThatThrownBy(() -> dataLinkService.removeDataLink(TEST_CONFIG_NAME))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data link 'test-config' is configured via Config and can't be removed");
    }

    @Test
    public void given_data_link_when_close_service_then_should_close_data_links() {
        DummyDataLink dataLink = dataLinkService.getDataLink(TEST_CONFIG_NAME);

        dataLinkService.close();

        assertThat(dataLink.isClosed())
                .describedAs("DataLink should have been closed")
                .isTrue();
    }

    @Test
    public void should_fail_when_non_existing_dataLink() {
        assertThatThrownBy(() -> dataLinkService.getDataLink("non-existing-data-link"))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Data link 'non-existing-data-link' not found");
    }

    private DataLinkService getDataLinkService() {
        instance = hazelcastInstanceFactory.newHazelcastInstance(config);
        return Util.getNodeEngine(instance).getDataLinkService();
    }

    public static class MissingImplementsDataLink {

        public MissingImplementsDataLink(DataLinkConfig config) {
        }
    }
}


