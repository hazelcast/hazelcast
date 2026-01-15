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
package com.hazelcast.spring.boot;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spring.HazelcastObjectExtractionConfiguration;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Auto Configuration for {@link HazelcastObjectExtractionConfiguration}, compatible with Spring Boot 4.
 * @since 5.7
 */
@Configuration(proxyBeanMethods = false)
@AutoConfiguration
@AutoConfigureAfter({
        org.springframework.boot.hazelcast.autoconfigure.HazelcastAutoConfiguration.class,
        com.hazelcast.spring.HazelcastExposeObjectRegistrar.class })
@Import(HazelcastObjectExtractionConfiguration.class)
public class HazelcastBoot4ObjectExtractionAutoConfiguration implements InitializingBean {

    private static final ILogger LOGGER = Logger.getLogger(HazelcastBoot4ObjectExtractionAutoConfiguration.class);

    private static final int REQUIRED_SPRING_BOOT_VERSION = 4;

    @Override
    public void afterPropertiesSet() {
        LOGGER.fine("After HazelcastBoot4ObjectExtractionAutoConfiguration initialization");
        String version = SpringBootVersion.getVersion();
        int detectedSpringMajorVersion = Integer.parseInt(version.split("\\.")[0]);

        if (detectedSpringMajorVersion != REQUIRED_SPRING_BOOT_VERSION) {
            String msg = "You're using hazelcast-spring-boot4 for Spring Boot 4.x, "
                    + "but your Spring Boot version is " + SpringBootVersion.getVersion() + ". "
                    + "Please use hazelcast-spring-boot" + detectedSpringMajorVersion + " instead.";
            throw new HazelcastException(msg);
        }
    }
}
