/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.ProtocolType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ServerSocketEndpointConfigTest {

    private ServerSocketEndpointConfig endpointConfig;
    private String endpointName = randomName();

    @Test
    public void testEndpointConfig_defaultConstructor() {
        endpointConfig = new ServerSocketEndpointConfig();
        endpointConfig.setName(endpointName);
        assertEquals(endpointName, endpointConfig.getName());
//        assertNull(endpointConfig.getMemberAddressProviderConfig());
        assertNull(endpointConfig.getProtocolType());
    }

    @Test
    public void testEndpointConfig_fullyConfigured() {
        endpointConfig = new ServerSocketEndpointConfig();
        endpointConfig.setName("anotherName")
//                      .setMemberAddressProviderConfig(new MemberAddressProviderConfig()
//                              .setEnabled(true)
//                              .setClassName("com.hazelcast.MemberAddressProviderImpl"))
                      .setProtocolType(ProtocolType.MEMBER)
                      .setPort(19000)
                      .setPublicAddress("192.168.2.1");

        assertEquals(endpointConfig.getProtocolType(), ProtocolType.MEMBER);
        assertEquals("anotherName", endpointConfig.getName());
        assertEquals(19000, endpointConfig.getPort());
        assertEquals("192.168.2.1", endpointConfig.getPublicAddress());
//        assertTrue(endpointConfig.getMemberAddressProviderConfig().isEnabled());
//        assertEquals("com.hazelcast.MemberAddressProviderImpl", endpointConfig.getMemberAddressProviderConfig().getClassName());
    }

}
