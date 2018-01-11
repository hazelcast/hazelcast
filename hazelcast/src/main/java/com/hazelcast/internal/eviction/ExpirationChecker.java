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

package com.hazelcast.internal.eviction;

/**
 * Interface for entries, records or whatever that can be checked for expiration.
 *
 * @param <E> Type of the {@link Expirable} value
 */
public interface ExpirationChecker<E extends Expirable> {

    /**
     * Checks if the given {@link Expirable} entry is expired or not.
     *
     * @param expirableEntry {@link Expirable} entry which is checked to see if it is expired or not.
     * @return <code>true</code> if the {@link Expirable} entry is expired, otherwise <code>false</code>.
     */
    boolean isExpired(E expirableEntry);

}
