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

package com.hazelcast.test.archunit;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.domain.JavaType;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.apache.commons.lang3.ClassUtils;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Set;

/** Asserts that config classes don't expose internal classes (i.e. those in an {@value #IMPL_PACKAGE_NAME} package) */
public class ConfigClassesExposingInternalImplementationCondition extends ArchCondition<JavaClass> {
    private static final String IMPL_PACKAGE_NAME = "impl";
    private static final String PRIVATE_API_ANNOTATION = "com.hazelcast.spi.annotation.PrivateApi";

    public ConfigClassesExposingInternalImplementationCondition() {
        super(MessageFormat.format("not have methods that expose internal classes from ''{0}'' packages", IMPL_PACKAGE_NAME));
    }

    @Override
    public void check(JavaClass configClass, ConditionEvents events) {
        // Ignore anonymous inner classes / lambdas
        if (configClass.isTopLevelClass() && !configClass.isAnnotatedWith(PRIVATE_API_ANNOTATION)) {
            configClass.getMethods()
                    .stream()
                    .filter(method -> method.getModifiers()
                            .contains(JavaModifier.PUBLIC) && !method.isAnnotatedWith(PRIVATE_API_ANNOTATION))
                    .forEach(method -> {
                        JavaType returnType = method.getReturnType();

                        method.getParameterTypes()
                                .stream()
                                .filter(ConfigClassesExposingInternalImplementationCondition::isImplPackage)
                                .forEach(parameter -> violated(events, configClass, method, parameter, "parameter"));

                        if (isImplPackage(returnType)) {
                            violated(events, configClass, method, returnType, "return type");
                        }
                    });
        }
    }

    private static void violated(ConditionEvents events, JavaClass configClass, JavaMethod method, JavaType exposedType,
            String reason) {
        if (!RuleException.contains(configClass, exposedType)) {
            String message = MessageFormat.format("""
                    "{0}#{1}" is part of the public API, \
                    but exposes internal class "{2}" (in an "{3}" package) via it''s {4}""", configClass.getName(),
                    method.getName(), exposedType.getName(), IMPL_PACKAGE_NAME, reason);
            events.add(SimpleConditionEvent.violated(configClass, message));
        }
    }

    private static boolean isImplPackage(JavaType type) {
        return type.toErasure()
                .getPackageName()
                .contains(ClassUtils.PACKAGE_SEPARATOR + IMPL_PACKAGE_NAME + ClassUtils.PACKAGE_SEPARATOR);
    }

    private record RuleException(String configClass, String exposedType) {
        /**
         * Existing cases we don't want to update for
         * <a href="https://github.com/hazelcast/hazelcast-mono/pull/2489#discussion_r1832436111">backwards compatibility</a>
         * reasons
         */
        private static final Collection<RuleException> EXCEPTIONS = Set.of(
                new RuleException("com.hazelcast.client.config.ClusterRoutingConfig",
                        "com.hazelcast.client.impl.connection.tcp.RoutingMode"),
                new RuleException("com.hazelcast.client.config.ProxyFactoryConfig",
                        "com.hazelcast.client.impl.spi.ClientProxyFactory"),
                new RuleException("com.hazelcast.config.CachePartitionLostListenerConfig",
                        "com.hazelcast.cache.impl.event.CachePartitionLostListener"),
                new RuleException("com.hazelcast.internal.config.CachePartitionLostListenerConfigReadOnly",
                        "com.hazelcast.cache.impl.event.CachePartitionLostListener"));

        private static boolean contains(JavaClass configClass, JavaType exposedType) {
            return EXCEPTIONS.contains(new RuleException(configClass.getName(), exposedType.getName()));
        }
    }
}
