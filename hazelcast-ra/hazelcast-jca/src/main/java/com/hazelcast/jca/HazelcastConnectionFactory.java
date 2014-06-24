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

package com.hazelcast.jca;

import javax.resource.ResourceException;
import javax.resource.cci.ConnectionFactory;
import javax.resource.cci.ConnectionSpec;

/**
 * Interface to allow safe access to Hazelcast's connection interface
 * without casting
 */
public interface HazelcastConnectionFactory extends ConnectionFactory {

    /**
     * @return access to the real bridging object to access Hazelcast's infrastructure
     * @see HazelcastConnection
     */
    HazelcastConnection getConnection() throws ResourceException;

    /**
     * @return access to the real bridging object to access Hazelcast's infrastructure
     * @see HazelcastConnection
     */
    HazelcastConnection getConnection(ConnectionSpec connSpec) throws ResourceException;
}
