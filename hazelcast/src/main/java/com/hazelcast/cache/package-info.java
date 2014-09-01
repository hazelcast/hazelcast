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
 * <p>
 *     Hazelcast JSR-107 aka JCache implementation
 * </p>
 */
package com.hazelcast.cache;
/*
<!--################################################################################################################-->
<!--Change the following properties on the command line to override with the coordinates for your implementation-->
<implementation-groupId>com.hazelcast</implementation-groupId>
<implementation-artifactId>hazelcast</implementation-artifactId>
<implementation-version>3.3-JCACHE-SNAPSHOT</implementation-version>

<!-- Change the following properties to your CacheManager and Cache implementation. Used by the unwrap tests. -->
<CacheManagerImpl>com.hazelcast.cache.impl.HazelcastCacheManager</CacheManagerImpl>
<CacheImpl>com.hazelcast.cache.ICache</CacheImpl>
<CacheEntryImpl>com.hazelcast.cache.impl.CacheEntry</CacheEntryImpl>

<!--Change the following to point to your MBeanServer, so that the TCK can resolve it. -->
<javax.management.builder.initial>com.hazelcast.cache.impl.TCKMBeanServerBuilder</javax.management.builder.initial>
<org.jsr107.tck.management.agentId>TCKMbeanServer</org.jsr107.tck.management.agentId>

<!--################################################################################################################-->
*/
