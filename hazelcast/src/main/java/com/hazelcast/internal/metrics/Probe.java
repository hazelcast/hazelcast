/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation that can be placed on a field or method of an object to indicate that it should be tracked by the
 * MetricsRegistry when the {@link MetricsRegistry#scanAndRegister(Object, String)} is called.
 *
 * The MetricsRegistry will automatically scan all interfaces and super classes of an object (recursively). So it is possible
 * to define a Gauge on e.g. an interface or abstract class.
 *
 * <h1>Prefer field</h1>
 * Prefer placing a Gauge to a field above a method if the type is a primitive. The {@link java.lang.reflect.Field}
 * provides access to the actual field without the need for autoboxing. With the {@link java.lang.reflect.Method} a wrapper
 * object is created when a primitive is returned by the method. Therefor fields produce less garbage than methods.
 *
 * A Gauge can be placed on field or methods with the following (return) type:
 * <ol>
 * <li>byte</li>
 * <li>short</li>
 * <li>int</li>
 * <li>float</li>
 * <li>long</li>
 * <li>double</li>
 * <li>AtomicInteger</li>
 * <li>AtomicLong</li>
 * <li>Counter</li>
 * <li>Byte</li>
 * <li>Short</li>
 * <li>Integer</li>
 * <li>Float</li>
 * <li>Double</li>
 * <li>Collection: it will return the size</li>
 * <li>Map: it will return the size</li>
 * </ol>
 *
 * If the field or method points to a null reference, it is interpreted as 0.
 */
@Retention(value = RUNTIME)
@Target({FIELD, METHOD })
public @interface Probe {

    /**
     * The name of the gauge. By default, the name of the field or method is used.
     *
     * @return the name of the gauge.
     */
    String name() default "";
}
