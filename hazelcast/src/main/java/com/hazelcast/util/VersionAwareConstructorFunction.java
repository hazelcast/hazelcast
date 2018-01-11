/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.version.Version;

/**
 * VersionAware version of the ConstructorFunction.
 * It is also able to create "default" objects when the version is unknown or not-specified. In this case
 * use the createNew method with out the version.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface VersionAwareConstructorFunction<K, V> extends ConstructorFunction<K, V> {

    /**
     * Creates a new instance of an object given the construction argument and the version of the object
     *
     * @param arg     construction argument
     * @param version version of the object it should create - it's cluster version bound, since objects change
     *                between release only
     * @return a new instance of an object
     */
    V createNew(K arg, Version version);

}
