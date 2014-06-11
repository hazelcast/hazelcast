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

/**
 * Contains the actual implementation of the {@link com.hazelcast.partition.InternalPartitionService}.
 *
 * It is very important that should be the least amount of dependencies to this package. Preferably only the
 * {@link com.hazelcast.partition.impl.InternalPartitionServiceImpl} is called while constructing an instance and after
 * that all communication should go through the {@link com.hazelcast.partition.InternalPartitionService} interface.
 *
 * Nobody needs to know about the actual implementing classes because this gives a very tight coupling and
 * this is undesirable. This is not only undesirable for external people because they could easily be tightly
 * coupled to the internals of Hazelcast, but it also is undesirable for us because it becomes a tangled mess
 * that prevent easy replacement/changes.
 */
package com.hazelcast.partition.impl;
