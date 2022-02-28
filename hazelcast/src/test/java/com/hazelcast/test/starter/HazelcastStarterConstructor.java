/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter;

import com.hazelcast.test.starter.HazelcastProxyFactory.ProxyPolicy;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation for constructor classes for {@link HazelcastProxyFactory}.
 * <p>
 * The annotated classes have to be in the package
 * {@code com.hazelcast.test.starter.constructor} to be registered.
 */
@Retention(value = RUNTIME)
@Target(TYPE)
public @interface HazelcastStarterConstructor {

    /**
     * The class names which this constructor class constructs.
     *
     * @return the supported class names
     */
    String[] classNames() default "";

    /**
     * The {@link ProxyPolicy} the supported classes should use.
     * <p>
     * Note: Classes which use {@link ProxyPolicy#NO_PROXY} have to implement
     * {@link com.hazelcast.internal.util.ConstructorFunction}. Classes which use
     * {@link ProxyPolicy#SUBCLASS_PROXY} can be an empty class.
     *
     * @return the {@link ProxyPolicy} of the supported classes
     */
    ProxyPolicy proxyPolicy() default ProxyPolicy.NO_PROXY;
}
