/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.HazelcastClientProxy;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.nio.ssl.TestKeyStoreUtil;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"clientNetworkConfig-applicationContext.xml"})
@Category(QuickTest.class)
public class TestClientNetworkConfig {

    @Resource(name = "client")
    private HazelcastClientProxy client;

    @BeforeClass
    @AfterClass
    public static void start() throws IOException {
        final String keyStoreFilePath = TestKeyStoreUtil.getKeyStoreFilePath();
        final String trustStoreFilePath = TestKeyStoreUtil.getTrustStoreFilePath();

        System.setProperty("test.keyStore", keyStoreFilePath);
        System.setProperty("test.trustStore", trustStoreFilePath);

        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void smokeMember() throws IOException {
        final int memberCountInConfigurationXml = 2;
        ClientConfig config = client.getClientConfig();
        assertEquals(memberCountInConfigurationXml
                , config.getNetworkConfig().getAddresses().size());
    }

    @Test
    public void smokeSocketOptions() throws IOException {
        final int bufferSizeInConfigurationXml = 32;
        ClientConfig config = client.getClientConfig();
        assertEquals(bufferSizeInConfigurationXml
                , config.getNetworkConfig().getSocketOptions().getBufferSize());
    }

    @Test
    public void smokeSocketInterceptor() throws IOException {
        ClientConfig config = client.getClientConfig();
        assertEquals(true
                , config.getNetworkConfig().getSocketInterceptorConfig().isEnabled());
    }

    @Test
    public void smokeSSLConfig() throws IOException {
        ClientConfig config = client.getClientConfig();
        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory"
                , config.getNetworkConfig().getSSLConfig().getFactoryClassName());
    }


}
