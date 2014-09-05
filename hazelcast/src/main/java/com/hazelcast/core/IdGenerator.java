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
 * Cluster-wide unique id generator.
 */
public interface IdGenerator extends DistributedObject {

    /**
     * Try to initialize this IdGenerator instance with given id. The first
     * generated id will be 1 bigger than id.
     *
     * @return true if initialization success. If id is equal or smaller
     * than 0, then false is returned.
     */
    boolean init(long id);

    /**
     * Generates and returns cluster-wide unique id.
     * Generated ids are guaranteed to be unique for the entire cluster
     * as long as the cluster is live. If the cluster restarts then
     * id generation will start from 0.
     *
     * @return cluster-wide new unique id
     */
    long newId();
}
