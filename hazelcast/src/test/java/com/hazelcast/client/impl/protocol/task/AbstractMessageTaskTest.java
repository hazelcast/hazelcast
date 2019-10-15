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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.codec.ClientCreateProxyCodec;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.util.JavaVersion;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.reflect.Whitebox;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.internal.util.JavaVersion.JAVA_11;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractMessageTaskTest {

    final Collection<Address> memberAddresses = new ArrayList<Address>();
    final Config config = new Config();
    final ClientCreateProxyCodec.RequestParameters parameters = new ClientCreateProxyCodec.RequestParameters();
    AbstractMessageTask messageTask;

    @Before
    public void setup() {
        assumeTrue("This test uses PowerMock Whitebox.setInternalState which fails in JDK >= 12", JavaVersion.isAtMost(JAVA_11));
        Node node = mock(Node.class);
        when(node.getConfig()).thenReturn(config);

        ClusterServiceImpl clusterService = mock(ClusterServiceImpl.class);
        when(clusterService.getMemberAddresses()).thenReturn(memberAddresses);

        messageTask = mock(AbstractMessageTask.class);
        when(messageTask.addressesDecodedWithTranslation()).thenCallRealMethod();

        Whitebox.setInternalState(node, "clusterService", clusterService);
        Whitebox.setInternalState(messageTask, "node", node);

        // setup common fields of parameters object
        parameters.name = "test";
        parameters.serviceName = MapService.SERVICE_NAME;
        Whitebox.setInternalState(messageTask, "parameters", parameters);
    }

    @Test
    public void assertionFalse_whenUnknownAddressesInDecodedMessage() throws UnknownHostException {
        config.getAdvancedNetworkConfig().setEnabled(true);
        memberAddresses.add(new Address("127.0.0.1", 5701));
        parameters.target = new Address("127.0.0.1", 65000);

        assertFalse(messageTask.addressesDecodedWithTranslation());
    }

    @Test
    public void assertionTrue_whenKnownAddressesInDecodedMessage() throws UnknownHostException {
        config.getAdvancedNetworkConfig().setEnabled(true);
        memberAddresses.add(new Address("127.0.0.1", 5701));
        parameters.target = new Address("127.0.0.1", 5701);

        assertTrue(messageTask.addressesDecodedWithTranslation());
    }

    @Test
    public void assertionTrue_whenAdvancedNetworkDisabled() throws UnknownHostException {
        config.getAdvancedNetworkConfig().setEnabled(false);
        memberAddresses.add(new Address("127.0.0.1", 5701));
        parameters.target = new Address("127.0.0.1", 65000);

        assertTrue(messageTask.addressesDecodedWithTranslation());
    }
}
