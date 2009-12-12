/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

public interface Instance {

    enum InstanceType {
        QUEUE, MAP, SET, LIST, LOCK, TOPIC, MULTIMAP, ID_GENERATOR
    }

    /**
     * Returns instance type such as map, set, list, lock, topic, multimap, id generator
     *
     * @return instance type
     */
    InstanceType getInstanceType();

    /**
     * Destroys this instance cluster-wide.
     * Clears and releases all resources for this instance.
     */
    void destroy();

    /**
     * Returns the unique id for this instance.
     * @return id the of this instance
     */
    Object getId();
}
