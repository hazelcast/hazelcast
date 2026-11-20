/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class BeanCreatorTest {

    private final DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    private final BeanCreator creator = new BeanCreator(beanFactory);
    private final RetrieveDistributedObjectFunction retrieve = mock(RetrieveDistributedObjectFunction.class);

    @Test
    void rejectsMissingInstancesBeforeRetrievingAnObject() {
        assertThatThrownBy(() -> creator.createBean("orders", retrieve, "member", "cluster"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No instances")
                .hasMessageContaining("instanceName=member")
                .hasMessageContaining("clusterName=cluster");
        verifyNoInteractions(retrieve);
    }

    @ParameterizedTest
    @CsvSource({"other-member, cluster", "member, other-cluster", "other-member, other-cluster"})
    void rejectsInstancesThatDoNotMatchTheRequestedConfiguration(String instanceName, String clusterName) {
        beanFactory.registerSingleton("member", member("member", "cluster"));

        assertThatThrownBy(() -> creator.createBean("orders", retrieve, instanceName, clusterName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No instances")
                .hasMessageContaining("instanceName=" + instanceName)
                .hasMessageContaining("clusterName=" + clusterName);
        verifyNoInteractions(retrieve);
    }

    @Test
    void selectsThePrimaryInstanceWhenMultipleInstancesMatch() {
        HazelcastInstance first = member(null, "cluster");
        registerPrimary("primaryMember", first);
        beanFactory.registerSingleton("otherMember", member(null, "cluster"));

        creator.createBean("orders", retrieve, null, "cluster");

        verify(retrieve).retrieve(first, "orders");
    }

    @Test
    void rejectsAmbiguousInstancesWhenNoneIsPrimary() {
        beanFactory.registerSingleton("firstMember", member(null, "cluster"));
        beanFactory.registerSingleton("secondMember", member(null, "cluster"));

        assertThatThrownBy(() -> creator.createBean("orders", retrieve, null, "cluster"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("More than one instance")
                .hasMessageContaining("instanceName=null")
                .hasMessageContaining("clusterName=cluster");
        verifyNoInteractions(retrieve);
    }

    @Test
    void rejectsAPrimaryInstanceFromAnotherClusterWhenMatchingMembersAreAmbiguous() {
        registerPrimary("primaryMember", member(null, "other-cluster"));
        beanFactory.registerSingleton("firstMember", member(null, "cluster"));
        beanFactory.registerSingleton("secondMember", member(null, "cluster"));

        assertThatThrownBy(() -> creator.createBean("orders", retrieve, null, "cluster"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("More than one instance");
        verifyNoInteractions(retrieve);
    }

    @Test
    void rejectsMultipleMatchingPrimaryMembers() {
        registerPrimary("firstPrimary", member(null, "cluster"));
        registerPrimary("secondPrimary", member(null, "cluster"));

        assertThatThrownBy(() -> creator.createBean("orders", retrieve, null, "cluster"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("More than one instance");
        verifyNoInteractions(retrieve);
    }

    @Test
    void rejectsAPrimaryClientWhenMultipleMembersMatch() {
        HazelcastInstance client = mock(HazelcastClientProxy.class);
        registerPrimary("primaryClient", client);
        beanFactory.registerSingleton("firstMember", member(null, "cluster"));
        beanFactory.registerSingleton("secondMember", member(null, "cluster"));

        assertThatThrownBy(() -> creator.createBean("orders", retrieve, null, "cluster"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("More than one instance");
        verifyNoInteractions(client, retrieve);
    }

    @Test
    void selectsTheNamedInstanceWithinTheSameCluster() {
        HazelcastInstance first = member("first", "cluster");
        HazelcastInstance second = member("second", "cluster");
        registerPrimary("firstMember", first);
        beanFactory.registerSingleton("secondMember", second);
        IMap<?, ?> orders = mock(IMap.class);
        when(second.getMap("orders")).thenAnswer(invocation -> orders);

        assertThat(creator.createBean("orders", HazelcastInstance::getMap, "second", "cluster")).isSameAs(orders);
        verify(first, never()).getMap("orders");
    }

    @Test
    void acceptsTheSameInstanceRegisteredUnderMultipleBeanNames() {
        HazelcastInstance member = member(null, "cluster");
        beanFactory.registerSingleton("member", member);
        beanFactory.registerSingleton("sameMember", member);
        IMap<?, ?> orders = mock(IMap.class);
        when(member.getMap("orders")).thenAnswer(invocation -> orders);

        assertThat(creator.createBean("orders", HazelcastInstance::getMap, null, "cluster")).isSameAs(orders);
        verify(member).getMap("orders");
    }

    private void registerPrimary(String beanName, HazelcastInstance instance) {
        RootBeanDefinition primary = new RootBeanDefinition(HazelcastInstance.class, () -> instance);
        primary.setPrimary(true);
        beanFactory.registerBeanDefinition(beanName, primary);
    }

    private static HazelcastInstance member(String instanceName, String clusterName) {
        HazelcastInstance member = mock(HazelcastInstance.class);
        when(member.getConfig()).thenReturn(new Config().setInstanceName(instanceName).setClusterName(clusterName));
        return member;
    }
}
