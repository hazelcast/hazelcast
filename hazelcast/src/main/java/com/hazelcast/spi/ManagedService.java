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

package com.hazelcast.spi;

import java.util.Properties;

/**
 * A interface that can be implemented by a SPI Service to receive lifecycle calls:
 * <ol>
 *     <li>initialization</li>
 *     <li>shutdown</li>
 *     <li>reset</li>
 * </ol>
 *
 * @author mdogan 7/23/12
 */
public interface ManagedService {

    /**
     * Initializes this ManagedService
     *
     * @param nodeEngine the NodeEngine this ManagedService belongs to.
     * @param properties the Properties. Can be used to pass settings to the service.
     */
    void init(NodeEngine nodeEngine, Properties properties);

    /**
     * reset this ManagedService back to initial state.
     *
     * todo: what is the purpose of reset
     */
    void reset();

    /**
     * Shuts down this ManagedService.
     *
     * todo: what is the purpose of the terminate variable.
     *
     * @param terminate
     */
    void shutdown(boolean terminate);
}
