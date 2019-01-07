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

package com.hazelcast.core;

/**
 * HazelcastJsonValue is a wrapper for Json formatted strings. It is preferred
 * to store HazelcastJsonValue instead of Strings for Json formatted strings.
 * Users can run predicates and use indexes on the attributes of the underlying
 * Json strings.
 *
 * HazelcastJsonValue is queried using Hazelcast's querying language.
 * See {@link com.hazelcast.query.Predicates}.
 *
 * In terms of querying, numbers in Json strings are treated as either
 * {@code Long} or {@code Double}. Strings, booleans and null are treated as
 * their Java counterparts.
 *
 * HazelcastJsonValue keeps given string as it is.
 *
 * See {@link com.hazelcast.json.HazelcastJson#fromString(String)}
 */
public interface HazelcastJsonValue {

    @Override
    String toString();

}
