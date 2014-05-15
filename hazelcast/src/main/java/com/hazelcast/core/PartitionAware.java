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
 * PartitionAware mean that data will be based in the same member based on partition key
 * and implementing tasks will be executed on the {@link #getPartitionKey()}'s owner member.
 * <p/>
 * This achieves data affinity. Data or execution occurs on the same partition.
 * <p/>
 * In Hazelcast, disparate data structures will be stored on the same partition, simply
 * based on partition key. So for example if "Steve" was used, then the following would be on one partition:
 * <ul>
 *     <li>a customers IMap with an entry of key "Steve</li>
 *     <li>an orders IMap using a customer key type implementing PartitionAware with key "Steve</li>
 *     <li>any queue named "Steve"</li>
 *     <li>any PartitionAware object with partition key "Steve</li>
 * </ul>
 *
 * So if you had a {@link com.hazelcast.core.IExecutorService} which needed to deal with a customer and a customer's
 * orders, optimal performance will be achieved by putting them on the same partition.
 * <p/>
 * {@link com.hazelcast.core.DistributedObject} also has a notion of partition key which is of type String. So
 * to ensure the same partition as distributed Objects Strings should be used for the partition key.
 * <p/>
 * @see com.hazelcast.core.DistributedObject
 * @param <T> key type
 */
public interface PartitionAware<T> {

    /**
     * The key that will be used by Hazelcast to specify the partition.
     * You should give the same key for objects that you want be in the same partition.
     *
     * @return partition key
     */
    T getPartitionKey();

}
