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

package com.hazelcast.spring.boot4;

import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.spring.CustomSpringExtension;
import com.hazelcast.spring.boot.HzConfProps;
import com.hazelcast.spring.boot.ObjectExportCompatibilityTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;

@TestPropertySource("classpath:test-params.properties")
public class Boot4ObjectExportCompatibilityTest extends ObjectExportCompatibilityTest {
    @Nested
    @SpringBootTest(classes =
            Boot4ObjectExportCompatibilityTest.NoAnnotationJustAutoConfiguration.Configuration.class)
    @ExtendWith({SpringExtension.class, CustomSpringExtension.class})
    @DisplayName("with no @ExposeHazelcastObjects, just autoconfiguration")
    class NoAnnotationJustAutoConfiguration extends BaseObjectExportTest {

        @Test
        void nothingIsExcluded() {
            Ringbuffer<?> ringbuffer = applicationContext.getBean(Ringbuffer.class);
            assertThat(ringbuffer.getName()).isEqualTo("ringbuffer");
        }

        @SpringBootApplication(scanBasePackages = "com.hazelcast.spring.boot.fortest")
        @EnableConfigurationProperties(HzConfProps.class)
        public static class Configuration extends ObjectExportConfigurationBase {
        }
    }

}
