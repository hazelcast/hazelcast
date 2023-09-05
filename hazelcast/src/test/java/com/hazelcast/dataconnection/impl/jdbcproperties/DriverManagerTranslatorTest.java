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

package com.hazelcast.dataconnection.impl.jdbcproperties;

import org.junit.Test;

import java.util.Properties;

import static com.hazelcast.dataconnection.impl.jdbcproperties.DataConnectionProperties.JDBC_URL;
import static com.hazelcast.dataconnection.impl.jdbcproperties.DataConnectionProperties.PASSWORD;
import static com.hazelcast.dataconnection.impl.jdbcproperties.DataConnectionProperties.USER;
import static com.hazelcast.dataconnection.impl.jdbcproperties.DriverManagerTranslator.translate;
import static org.assertj.core.api.Assertions.assertThat;

public class DriverManagerTranslatorTest {

    @Test
    public void testTranslatableProperties() {
        Properties hzProperties = new Properties();
        String jdbcUrl = "jdbcUrl";
        String user = "user";
        String password = "password";
        String myProperty = "5000";


        hzProperties.put(JDBC_URL, jdbcUrl);
        hzProperties.put(USER, user);
        hzProperties.put(PASSWORD, password);
        hzProperties.put("myProperty", myProperty);


        Properties driverManagerProperties = translate(hzProperties);

        assertThat(driverManagerProperties).doesNotContainKey(JDBC_URL);
        assertThat(driverManagerProperties.getProperty("user")).isEqualTo(user);
        assertThat(driverManagerProperties.getProperty("password")).isEqualTo(password);
        assertThat(driverManagerProperties.getProperty("myProperty")).isEqualTo(myProperty);
        assertThat(driverManagerProperties).hasSize(3);
    }

    @Test
    public void testDriverSpecificProperty() {
        Properties hzProperties = new Properties();

        String myProperty = "myProperty";
        hzProperties.put(myProperty, myProperty);

        Properties driverManagerProperties = translate(hzProperties);
        assertThat(driverManagerProperties.getProperty(myProperty)).isEqualTo(myProperty);
    }

}
