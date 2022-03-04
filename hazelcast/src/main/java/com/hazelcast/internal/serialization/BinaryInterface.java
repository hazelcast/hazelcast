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

package com.hazelcast.internal.serialization;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Indicates that the binary serialized form of the annotated class is used in client-member communication over Hazelcast
 * Open Binary Client Protocol or in other ways which may break Hazelcast or other systems. Changing the way
 * {@code @BinaryInterface} classes are serialized will result in incompatibilities, so the following rules
 * apply to classes annotated with this annotation in the 3.x release series:
 * <p>
 * - NEVER CHANGE THEM<br>
 * - NEVER MAKE THEM IMPLEMENT THE VERSIONED INTERFACE
 * <p>
 * For the purposes of serializable classes conventions testing, this annotation is only taken into account when
 * used on concrete classes; it does not make sense to annotate an interface or an abstract class, because serialized form
 * is only relevant in the context of a concrete class. However, it may be informative to use this annotation also on
 * interfaces or abstract classes, as a warning to implementers.
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface BinaryInterface {

}
