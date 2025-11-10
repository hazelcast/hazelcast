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
package com.hazelcast.spring;

import com.hazelcast.map.IMap;
import org.springframework.context.annotation.Import;
import org.springframework.core.type.AnnotationMetadata;

import javax.annotation.Nonnull;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashSet;
import java.util.Set;

/**
 * Annotation that instruments Spring to expose Hazelcast objects as beans.
 * <p>Using this annotation on your configuration will register beans of:
 * <ul>
 *     <li>JetService
 *     <li>SqlService
 *     <li>Data structures, such as {@link IMap}, {@link com.hazelcast.replicatedmap.ReplicatedMap},
 *     {@link com.hazelcast.topic.ITopic}.
 * </ul>
 *
 * Data structures will be exposed if configured:
 * <ul>
 *     <li>via static Java configuration (when starting the cluster with new {@link com.hazelcast.config.Config} object).
 *     <li>via dynamic Java configuration (e.g. when configured using {@code instance.getConfig().addMapConfig}).
 *          Note that the autowiring is done once; therefore dynamically added configuration after
 *          {@link org.springframework.context.ApplicationContext}
 *          is initialized on one member will be used to autowire objects
 *          on newly created member, but not on the member that added the dynamic configuration
 *     <li>via XML/YAML static configuration
 * </ul>
 *
 * If there is already a bean named the same as some Hazelcast object, the bean with Hazelcast object will not be registered.
 * @since 5.6
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import({ HazelcastExposeObjectRegistrar.class, HazelcastObjectExtractionConfiguration.class })
public @interface ExposeHazelcastObjects {

    /**
     * Specifies which (by name) objects will be exposed as Spring beans.
     *
     * <p>By default, all objects listed in {@link ExposeHazelcastObjects} are exposed, this option allows user to narrow
     * the functionality to only some part of objects.
     */
    String[] includeByName() default {};

    /**
     * Specifies which objects will <b>not</b> be exposed as Spring beans.
     *
     * <p>By default, all objects listed in {@link ExposeHazelcastObjects} are exposed, this option allows user to narrow
     * the functionality and exclude unwanted objects from being added as Spring beans.
     */
    String[] excludeByName() default {};

    /**
     * Specifies which types of objects will be exposed as Spring beans.
     * <p>
     * By default, all objects will be included.
     */
    Class<?>[] includeByType() default {};

    /**
     * Specifies which types of objects won't be exposed as Spring beans.
     * <p>
     * By default, no type is excluded.
     */
    Class<?>[] excludeByType() default {};

    record Configuration (Set<String> includeByName, Set<String> excludeByName,
                          Set<Class<?>> includeByType, Set<Class<?>> excludeByType) {

        static Configuration toConfiguration(@Nonnull AnnotationMetadata importingClassMetadata) {
            var metadata =  importingClassMetadata.getAnnotations().get(ExposeHazelcastObjects.class);
            if (!metadata.isPresent()) {
                return Configuration.empty();
            }
            String[] includeByName = metadata.getValue("includeByName", String[].class).orElse(new String[0]);
            String[] excludeByName = metadata.getValue("excludeByName", String[].class).orElse(new String[0]);
            Class<?>[] includeByType = metadata.getValue("includeByType", Class[].class).orElse(new Class<?>[0]);
            Class<?>[] excludeByType = metadata.getValue("excludeByType", Class[].class).orElse(new Class<?>[0]);
            return new Configuration(Set.of(includeByName), Set.of(excludeByName), Set.of(includeByType), Set.of(excludeByType));
        }

        public static Configuration empty() {
            return new Configuration(new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>());
        }

        boolean canInclude(String beanName) {
            return (includeByName.isEmpty() || includeByName.contains(beanName)) && !excludeByName.contains(beanName);
        }

        boolean canInclude(Class<?> beanClass) {
            var includedByType = this.includeByType.stream().anyMatch(beanClass::isAssignableFrom);
            var excludedByType = this.excludeByType.stream().anyMatch(beanClass::isAssignableFrom);
            return (includeByType.isEmpty() || includedByType) && !excludedByType;
        }

        void includeOther(Configuration other) {
            includeByName.addAll(other.includeByName());
            excludeByName.addAll(other.excludeByName());
            includeByType.addAll(other.includeByType());
            excludeByType.addAll(other.excludeByType());
        }
    }

}
