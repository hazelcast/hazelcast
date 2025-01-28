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

/** Asserts that public API classes don't expose internal classes */
public class PublicApiClassesExposingInternalImplementationCondition extends ArchCondition<JavaClass> {
    private static final Set<String> INTERNAL_PACKAGE_NAMES = Set.of("impl", "internal");
    private static final String PRIVATE_API_ANNOTATION = "com.hazelcast.spi.annotation.PrivateApi";

    public PublicApiClassesExposingInternalImplementationCondition() {
        super(MessageFormat.format("not have methods that expose internal classes from these packages: ''{0}''",
                String.join(", ", INTERNAL_PACKAGE_NAMES)));
    }

    @Override
    public void check(JavaClass publicClass, ConditionEvents events) {
        // Ignore anonymous inner classes / lambdas
        if (publicClass.isTopLevelClass() && !publicClass.isAnnotatedWith(PRIVATE_API_ANNOTATION)) {
            publicClass.getMethods()
                    .stream()
                    .filter(method -> method.getModifiers()
                            .contains(JavaModifier.PUBLIC) && !method.isAnnotatedWith(PRIVATE_API_ANNOTATION))
                    .forEach(method -> {
                        JavaType returnType = method.getReturnType();

                        method.getParameterTypes()
                                .stream()
                                .flatMap(type -> type.getAllInvolvedRawTypes().stream())
                                .filter(PublicApiClassesExposingInternalImplementationCondition::isInternalPackage)
                                .forEach(parameter -> violated(events, publicClass, method, parameter, "parameter"));

                        returnType.getAllInvolvedRawTypes()
                                  .stream()
                                  .filter(PublicApiClassesExposingInternalImplementationCondition::isInternalPackage)
                                  .forEach(type -> violated(events, publicClass, method, type, "return type"));
                    });
        }
    }

    private static void violated(ConditionEvents events, JavaClass publicClass, JavaMethod method, JavaType exposedType,
            String reason) {
        if (isNotRuleException(publicClass, exposedType)) {
            String message = MessageFormat.format("""
                    "{0}#{1}" is part of the public API, \
                    but exposes internal class "{2}" via it''s {3}""", publicClass.getName(),
                    method.getName(), exposedType.getName(), reason);
            events.add(SimpleConditionEvent.violated(publicClass, message));
        }
    }

    private static boolean isNotRuleException(JavaClass publicClass, JavaType exposedType) {
        // Grant exception for all `com.hazelcast.internal.json` exposed types, as this is widely used as of 5.5
        return !exposedType.getName().startsWith("com.hazelcast.internal.json")
                && !RuleException.contains(publicClass, exposedType);
    }

    private static boolean isInternalPackage(JavaType type) {
        for (String packageName : INTERNAL_PACKAGE_NAMES) {
            if (type.toErasure().getPackageName()
                    .contains(ClassUtils.PACKAGE_SEPARATOR + packageName + ClassUtils.PACKAGE_SEPARATOR)) {
                return true;
            }
        }
        return false;
    }

    private record RuleException(String configClass, String exposedType) {
        /**
         * Existing cases we don't want to update for
         * <a href="https://github.com/hazelcast/hazelcast-mono/pull/2489#discussion_r1832436111">backwards compatibility</a>
         * reasons.
         * TODO: Consider fixing the exposure of internal classes in these historical locations in the future
         */
        private static final Collection<RuleException> EXCEPTIONS = Set.of(
                new RuleException("com.hazelcast.client.config.ClusterRoutingConfig",
                        "com.hazelcast.client.impl.connection.tcp.RoutingMode"),
                new RuleException("com.hazelcast.client.config.ProxyFactoryConfig",
                        "com.hazelcast.client.impl.spi.ClientProxyFactory"),
                new RuleException("com.hazelcast.config.CachePartitionLostListenerConfig",
                        "com.hazelcast.cache.impl.event.CachePartitionLostListener"),
                new RuleException("com.hazelcast.config.EvictionConfig",
                        "com.hazelcast.internal.eviction.EvictionStrategyType"),
                new RuleException("com.hazelcast.cache.ICache",
                        "com.hazelcast.cache.impl.event.CachePartitionLostListener"),
                new RuleException("com.hazelcast.jet.core.processor.SinkProcessors",
                        "com.hazelcast.jet.impl.connector.MapSinkConfiguration"),
                new RuleException("com.hazelcast.nio.serialization.SerializerHook",
                        "com.hazelcast.internal.serialization.SerializationService"),
                new RuleException("com.hazelcast.security.SecurityContext",
                        "com.hazelcast.internal.nio.Connection"),
                new RuleException("com.hazelcast.security.SecurityContext",
                        "com.hazelcast.sql.impl.security.SqlSecurityContext"),
                new RuleException("com.hazelcast.spi.merge.HyperLogLogMergePolicy",
                        "com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog"),
                new RuleException("com.hazelcast.wan.WanMigrationAwarePublisher",
                        "com.hazelcast.internal.partition.PartitionMigrationEvent"),
                new RuleException("com.hazelcast.wan.WanMigrationAwarePublisher",
                        "com.hazelcast.internal.partition.PartitionReplicationEvent"),
                new RuleException("com.hazelcast.wan.WanMigrationAwarePublisher",
                        "com.hazelcast.internal.services.ServiceNamespace"));

        private static boolean contains(JavaClass configClass, JavaType exposedType) {
            return EXCEPTIONS.contains(new RuleException(configClass.getName(), exposedType.getName()));
        }
    }
}
