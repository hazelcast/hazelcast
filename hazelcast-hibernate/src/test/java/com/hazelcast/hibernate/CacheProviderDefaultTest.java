/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.hibernate;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.hibernate.provider.HazelcastCacheProvider;
import com.hazelcast.impl.GroupProperties;
import org.hibernate.cfg.Environment;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import java.util.Properties;

@RunWith(TestBlockJUnit4ClassRunner.class)
@Ignore
public class CacheProviderDefaultTest extends HibernateStatisticsTestSupport {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        Hazelcast.shutdownAll();
    }

    protected Properties getCacheProperties() {
        Properties props = new Properties();
        props.setProperty(Environment.CACHE_PROVIDER, HazelcastCacheProvider.class.getName());
        return props;
    }
}
