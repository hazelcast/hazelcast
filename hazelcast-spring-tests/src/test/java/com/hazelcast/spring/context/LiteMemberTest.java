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

package com.hazelcast.spring.context;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.CustomSpringExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"test-lite-member-application-context.xml"})
class LiteMemberTest {

    @BeforeAll
    @AfterAll
    public static void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Autowired
    private HazelcastInstance instance;

    @Test
    void shouldBeLiteMember() {
        assertTrue(instance.getConfig().isLiteMember(), "Expected Config.isLiteMember() to be true");
        assertTrue(instance.getCluster().getLocalMember().isLiteMember(), "Expected Member.isLiteMember() to be true");
    }
}
