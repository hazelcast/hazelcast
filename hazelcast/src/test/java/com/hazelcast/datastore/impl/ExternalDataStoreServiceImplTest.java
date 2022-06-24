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
package com.hazelcast.datastore.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExternalDataStoreConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.datastore.ExternalDataStoreFactory;
import com.hazelcast.datastore.ExternalDataStoreService;
import com.hazelcast.datastore.JdbcDataStoreFactory;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExternalDataStoreServiceImplTest extends HazelcastTestSupport {

    private final Config config = new Config();

    @Before
    public void configure() {
        Properties properties = new Properties();
        properties.put("jdbc.url", "jdbc:h2:mem:" + ExternalDataStoreServiceImplTest.class.getSimpleName());
        ExternalDataStoreConfig externalDataStoreConfig = new ExternalDataStoreConfig()
                .setName("test-data-store")
                .setClassName("com.hazelcast.datastore.JdbcDataStoreFactory")
                .setProperties(properties);
        config.getExternalDataStoreConfigs().put("test-data-store", externalDataStoreConfig);
    }

    @Test
    public void should_return_working_datastore() throws Exception {
        HazelcastInstance instance = createHazelcastInstance(config);
        ExternalDataStoreService externalDataStoreService = Util.getNodeEngine(instance).getExternalDataStoreService();
        ExternalDataStoreFactory<?> dataStoreFactory = externalDataStoreService.getExternalDataStoreFactory("test-data-store");
        assertInstanceOf(JdbcDataStoreFactory.class, dataStoreFactory);

        DataSource dataSource = ((JdbcDataStoreFactory) dataStoreFactory).createDataStore();

        ResultSet resultSet = dataSource.getConnection().prepareStatement("select 'some-name' as name").executeQuery();
        resultSet.next();
        String actualName = resultSet.getString(1);

        assertThat(actualName).isEqualTo("some-name");
    }

    @Test
    public void should_fail_when_non_existing_datastore() throws Exception {
        HazelcastInstance instance = createHazelcastInstance(config);
        ExternalDataStoreService externalDataStoreService = Util.getNodeEngine(instance).getExternalDataStoreService();
        assertThatThrownBy(() -> externalDataStoreService.getExternalDataStoreFactory("non-existing-data-store"))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("External data store 'non-existing-data-store' not found");
    }
}


