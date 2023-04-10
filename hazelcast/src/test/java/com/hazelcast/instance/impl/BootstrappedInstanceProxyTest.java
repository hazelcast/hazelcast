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

package com.hazelcast.instance.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.AbstractJetInstance;
import org.junit.Test;
import org.mockito.Mockito;

import static com.hazelcast.instance.impl.BootstrappedInstanceProxyFactory.createWithMemberJetProxy;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

public class BootstrappedInstanceProxyTest {

    @Test
    public void testShutdownNotAllowed() {
        HazelcastInstance hazelcastInstance = Mockito.mock(HazelcastInstance.class);
        AbstractJetInstance abstractJetInstance = Mockito.mock(AbstractJetInstance.class);
        when(hazelcastInstance.getJet()).thenReturn(abstractJetInstance);
        // When shutdown is called throw exception
        when(hazelcastInstance.getLifecycleService()).thenThrow(new IllegalStateException());

        BootstrappedInstanceProxy bootstrappedInstanceProxy = createWithMemberJetProxy(hazelcastInstance);


        bootstrappedInstanceProxy.setShutDownAllowed(false);
        bootstrappedInstanceProxy.shutdown();

        // Shutdown is not allowed so no exception should be thrown
        assertThatCode(() -> bootstrappedInstanceProxy.shutdown())
                .doesNotThrowAnyException();
    }

    @Test
    public void testShutdownAllowed() {
        HazelcastInstance hazelcastInstance = Mockito.mock(HazelcastInstance.class);
        AbstractJetInstance abstractJetInstance = Mockito.mock(AbstractJetInstance.class);
        when(hazelcastInstance.getJet()).thenReturn(abstractJetInstance);
        // When shutdown is called throw exception
        when(hazelcastInstance.getLifecycleService()).thenThrow(new IllegalStateException());

        BootstrappedInstanceProxy bootstrappedInstanceProxy = createWithMemberJetProxy(hazelcastInstance);
        bootstrappedInstanceProxy.setShutDownAllowed(true);

        // Shutdown is allowed so exception should be thrown
        assertThatThrownBy(() -> bootstrappedInstanceProxy.shutdown())
                .isInstanceOf(IllegalStateException.class);
    }
}
