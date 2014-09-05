/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

/**
 * Container managed context, such as Spring, Guice and etc.
 *
 */

public interface ManagedContext {

    /**
     * Initialize the given object instance.
     * This is intended for repopulating select fields and methods for deserialized instances.
     * It is also possible to proxy the object, e.g. with AOP proxies.
     *
     * @param obj Object to initialize
     * @return the initialized object to use
     */
    Object initialize(Object obj);

}
