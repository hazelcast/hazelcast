/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization.impl;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static com.hazelcast.nio.serialization.impl.BinaryInterface.Reason.CLIENT_COMPATIBILITY;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Indicates that the binary serialized form of the annotated class is used in client-member communication over Hazelcast
 * Open Binary Client Protocol or in other ways which may break Hazelcast or other systems. Changing the way
 * {@code @BinaryInterface} classes are serialized will result in incompatibilities, so the following rules
 * apply to classes annotated with this annotation in the 3.x release series:
 *
 * - NEVER CHANGE THEM
 * - NEVER MAKE THEM IMPLEMENT THE VERSIONED INTERFACE
 *
 * For the purposes of serializable classes conventions testing, this annotation is only taken into account when
 * used on concrete classes; it does not make sense to annotate an interface or an abstract class, because serialized form
 * is only relevant in the context of a concrete class. However, it may be informative to use this annotation also on
 * interfaces or abstract classes, as a warning to implementers.
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface BinaryInterface {

    /**
     * Describe reasoning for annotating class with {@code @BinaryInterface}
     * @return reason why a class is annotated
     */
    Reason reason() default CLIENT_COMPATIBILITY;

    public enum Reason {
        /**
         * Class is used in client-member communication, therefore changing its serialized form will break
         * client-member compatibility.
         */
        CLIENT_COMPATIBILITY,
        /**
         * Class is not used in client-member communication, however is part of public API so cannot be
         * migrated to {@code IdentifiedDataSerializable}.
         */
        PUBLIC_API,
        /**
         * Class may or may not be used in serialized form in the context of Hazelcast however is {@code Serializable}
         * due to conventions and cannot be converted to {@code IdentifiedDataSerializable}. Examples:
         * <ul>
         *     <li>inheritance from a class external to Hazelcast e.g. {@code CacheEntryEvent}, which imposes
         *     serializability even when not required</li>
         *     <li>inheritance from a class external to Hazelcast, serializability may be desired, however
         *     {@code IdentifiedDataSerializable} requires a default no-args constructor and non-final fields which
         *     is not always possible (e.g. subclasses of {@code java.util.EventObject} & {@code java.lang.Exception})</li>
         *     <li>a {@code Comparator} which should by convention also implement {@code Serializable}.</li>
         * </ul>
         */
        OTHER_CONVENTION,

    }

}
