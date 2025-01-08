/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.security;

import com.hazelcast.config.Config;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.IPermissionPolicy;
import com.hazelcast.security.SecurityInterceptor;
import com.hazelcast.spring.CustomSpringExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"secure-applicationContext-hazelcast.xml"})
class SecureApplicationContextTest {

    @Autowired
    private Config config;

    private SecurityConfig securityConfig;

    @Autowired
    private ICredentialsFactory dummyCredentialsFactory;

    @Autowired
    private IPermissionPolicy dummyPermissionPolicy;

    @BeforeEach
    void init() {
        securityConfig = config.getSecurityConfig();
    }

    @Test
    void testBasics() {
        assertNotNull(securityConfig);
        assertTrue(securityConfig.isEnabled());
        assertTrue(securityConfig.getClientBlockUnmappedActions());
        assertEquals(2, securityConfig.getRealmConfigs().size());
        assertNotNull(securityConfig.getClientRealm());
        assertNotNull(securityConfig.getClientPermissionConfigs());
        assertFalse(securityConfig.getClientPermissionConfigs().isEmpty());
        assertNotNull(securityConfig.getMemberRealm());
        assertNotNull(securityConfig.getClientPolicyConfig());
        assertEquals(1, securityConfig.getSecurityInterceptorConfigs().size());
    }

    @Test
    void testMemberRealm() {
        RealmConfig realmConfig = securityConfig.getRealmConfig(securityConfig.getMemberRealm());
        JaasAuthenticationConfig jaasAuthenticationConfig = realmConfig.getJaasAuthenticationConfig();
        assertNotNull(jaasAuthenticationConfig);
        List<LoginModuleConfig> list = jaasAuthenticationConfig.getLoginModuleConfigs();
        assertEquals(1, list.size());
        LoginModuleConfig lm = list.get(0);
        assertEquals("org.example.EmptyLoginModule", lm.getClassName());
        assertFalse(lm.getProperties().isEmpty());
        assertEquals(LoginModuleUsage.REQUIRED, lm.getUsage());

        CredentialsFactoryConfig credentialsFactoryConfig = realmConfig.getCredentialsFactoryConfig();
        assertNotNull(credentialsFactoryConfig);
        assertEquals(dummyCredentialsFactory, credentialsFactoryConfig.getImplementation());
    }

    @Test
    void testClientLoginConfigs() {
        RealmConfig realmConfig = securityConfig.getRealmConfig(securityConfig.getClientRealm());
        JaasAuthenticationConfig jaasAuthenticationConfig = realmConfig.getJaasAuthenticationConfig();
        assertNotNull(jaasAuthenticationConfig);
        List<LoginModuleConfig> list = jaasAuthenticationConfig.getLoginModuleConfigs();
        assertEquals(2, list.size());
        LoginModuleConfig lm1 = list.get(0);
        assertEquals("com.hazelcast.examples.MyOptionalLoginModule", lm1.getClassName());
        assertFalse(lm1.getProperties().isEmpty());
        assertEquals(LoginModuleUsage.OPTIONAL, lm1.getUsage());
        LoginModuleConfig lm2 = list.get(1);
        assertEquals("org.example.EmptyLoginModule", lm2.getClassName());
        assertFalse(lm2.getProperties().isEmpty());
        assertEquals(LoginModuleUsage.REQUIRED, lm2.getUsage());
    }

    @Test
    void testPermissionPolicy() {
        assertEquals("com.hazelcast.examples.MyPermissionPolicy", securityConfig.getClientPolicyConfig().getClassName());
        assertFalse(securityConfig.getClientPolicyConfig().getProperties().isEmpty());
        assertEquals(dummyPermissionPolicy, securityConfig.getClientPolicyConfig().getImplementation());
    }

    @Test
    void testPermissions() {
        Set<PermissionConfig> perms = securityConfig.getClientPermissionConfigs();
        assertFalse(perms.isEmpty());
        for (PermissionConfig permConfig : perms) {
            switch (permConfig.getType()) {
                case ALL:
                    assertEquals("admin", permConfig.getPrincipal());
                    assertEquals(1, permConfig.getEndpoints().size());
                    assertEquals("127.0.0.1", permConfig.getEndpoints().iterator().next());
                    break;
                case MAP:
                    assertEquals("customMap", permConfig.getName());
                    assertEquals("dev", permConfig.getPrincipal());
                    assertEquals(1, permConfig.getEndpoints().size());
                    assertEquals("127.0.0.1", permConfig.getEndpoints().iterator().next());
                    break;
                case QUEUE:
                    assertEquals("customQ", permConfig.getName());
                    assertEquals("dev", permConfig.getPrincipal());
                    assertEquals(1, permConfig.getEndpoints().size());
                    assertEquals("127.0.0.1", permConfig.getEndpoints().iterator().next());
                    break;
                case CACHE:
                    assertEquals("test-cache", permConfig.getName());
                    assertEquals("dev", permConfig.getPrincipal());
                    assertEquals(1, permConfig.getEndpoints().size());
                    assertEquals("127.0.0.1", permConfig.getEndpoints().iterator().next());
                    assertEquals(4, permConfig.getActions().size());
                    String[] expectedActions = new String[]{"create", "add", "read", "destroy"};
                    String[] actualActions = permConfig.getActions().toArray(new String[0]);
                    assertArrayEquals(expectedActions, actualActions);
                    break;
                case CONFIG:
                    assertEquals("dev", permConfig.getPrincipal());
                    assertEquals(1, permConfig.getEndpoints().size());
                    assertEquals("127.0.0.1", permConfig.getEndpoints().iterator().next());
                    break;
            }
        }
    }

    @Test
    void testSecurityInterceptors() {
        List<SecurityInterceptorConfig> interceptorConfigs = securityConfig.getSecurityInterceptorConfigs();
        assertEquals(1, interceptorConfigs.size());
        SecurityInterceptorConfig interceptorConfig = interceptorConfigs.get(0);
        String className = interceptorConfig.getClassName();
        assertEquals(DummySecurityInterceptor.class.getName(), className);
        SecurityInterceptor securityInterceptor = interceptorConfig.getImplementation();
        assertInstanceOf(DummySecurityInterceptor.class, securityInterceptor);
    }
}
