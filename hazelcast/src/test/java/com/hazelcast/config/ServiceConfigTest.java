/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 6/24/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ServiceConfigTest extends HazelcastTestSupport {

    @Test
    public void testXml() {
        Config config = new ClasspathXmlConfig("com/hazelcast/config/hazelcast-service.xml");
        ServiceConfig serviceConfig = config.getServicesConfig().getServiceConfig("my-service");
        assertEquals("com.hazelcast.examples.MyService", serviceConfig.getClassName());

        MyServiceConfig configObject = (MyServiceConfig) serviceConfig.getConfigObject();
        assertNotNull(configObject);
        assertEquals("prop1", configObject.stringProp);
        assertEquals(123, configObject.intProp);
        assertTrue(configObject.boolProp);
    }


    @Test
    public void testService() {
        Config config = new Config();
        MyServiceConfig configObject = new MyServiceConfig();
        MyService service = new MyService();
        config.getServicesConfig().addServiceConfig(new ServiceConfig().setEnabled(true)
                .setName("my-service").setConfigObject(configObject).setImplementation(service));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        factory.newHazelcastInstance(config);

        assertTrue(configObject == service.config);
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(ServiceConfig.class)
                .allFieldsShouldBeUsed()
                .suppress(Warning.NONFINAL_FIELDS, Warning.NULL_FIELDS)
                .verify();
    }

}
