/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import org.junit.Test;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

public class InvalidApplicationContextTest {

    @Test
    public void shouldFailWhenLoadBalancerContainsClassNameAndImplementationAttribute() {
        String expectedMessage = "Unexpected exception parsing XML document from class path resource [com/hazelcast/spring/customLoadBalancer-invalidApplicationContext.xml]";

        String rootExpectedMessage = "Exactly one of 'class-name' or 'implementation' attributes is required to create LoadBalancer!";
        BeanDefinitionStoreException exception = assertThrows(BeanDefinitionStoreException.class,
                () -> new ClassPathXmlApplicationContext("com\\hazelcast\\spring\\customLoadBalancer-invalidApplicationContext.xml")
        );
        assertThat(exception)
                .hasMessageContaining(expectedMessage)
                .hasRootCauseMessage(rootExpectedMessage);
    }
}
