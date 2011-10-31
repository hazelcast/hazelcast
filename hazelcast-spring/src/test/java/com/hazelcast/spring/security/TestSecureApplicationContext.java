/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.spring.security;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Set;

import javax.annotation.Resource;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.config.*;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.IPermissionPolicy;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "secure-applicationContext-hazelcast.xml" })
public class TestSecureApplicationContext {

	@Resource
	private Config config;

	private SecurityConfig securityConfig;

	@Resource
	private ICredentialsFactory dummyCredentialsFactory;

	@Resource
	private IPermissionPolicy dummyPermissionPolicy;

	@BeforeClass
	@AfterClass
	public static void start() {
		Hazelcast.shutdownAll();
	}

	@Before
	public void init() {
		securityConfig = config.getSecurityConfig();
	}

	@Test
	public void testBasics() {
		assertNotNull(securityConfig);
		assertTrue(securityConfig.isEnabled());
		assertNotNull(securityConfig.getClientLoginModuleConfigs());
		assertFalse(securityConfig.getClientLoginModuleConfigs().isEmpty());
		assertNotNull(securityConfig.getClientPermissionConfigs());
		assertFalse(securityConfig.getClientPermissionConfigs().isEmpty());
		assertNotNull(securityConfig.getMemberLoginModuleConfigs());
		assertFalse(securityConfig.getMemberLoginModuleConfigs().isEmpty());
		assertNotNull(securityConfig.getClientPolicyConfig());
		assertNotNull(securityConfig.getMemberCredentialsConfig());
	}

	@Test
	public void testMemberLoginConfigs() {
		List<LoginModuleConfig> list = securityConfig.getMemberLoginModuleConfigs();
		assertTrue(list.size() == 1);
		LoginModuleConfig lm = list.get(0);
		assertEquals("com.hazelcast.examples.MyRequiredLoginModule", lm.getClassName());
		assertFalse(lm.getProperties().isEmpty());
		assertEquals(LoginModuleUsage.REQUIRED, lm.getUsage());
	}

	@Test
	public void testClientLoginConfigs() {
		List<LoginModuleConfig> list = securityConfig.getClientLoginModuleConfigs();
		assertTrue(list.size() == 2);

		LoginModuleConfig lm1 = list.get(0);
		assertEquals("com.hazelcast.examples.MyOptionalLoginModule", lm1.getClassName());
		assertFalse(lm1.getProperties().isEmpty());
		assertEquals(LoginModuleUsage.OPTIONAL, lm1.getUsage());

		LoginModuleConfig lm2 = list.get(1);
		assertEquals("com.hazelcast.examples.MyRequiredLoginModule", lm2.getClassName());
		assertFalse(lm2.getProperties().isEmpty());
		assertEquals(LoginModuleUsage.REQUIRED, lm2.getUsage());
	}

	@Test
	public void testCredentialsFactory() {
		assertEquals("com.hazelcast.examples.MyCredentialsFactory", securityConfig.getMemberCredentialsConfig()
				.getClassName());
		assertFalse(securityConfig.getMemberCredentialsConfig().getProperties().isEmpty());
		assertEquals(dummyCredentialsFactory, securityConfig.getMemberCredentialsConfig().getImplementation());
	}

	@Test
	public void testPermissionPolicy() {
		assertEquals("com.hazelcast.examples.MyPermissionPolicy", securityConfig.getClientPolicyConfig().getClassName());
		assertFalse(securityConfig.getClientPolicyConfig().getProperties().isEmpty());
		assertEquals(dummyPermissionPolicy, securityConfig.getClientPolicyConfig().getImplementation());
	}

	@Test
	public void testPermissions() {
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
			}
		}
	}

}
