/*
 * Copyright 2024 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.mapstore.mongodb;

import com.hazelcast.mapstore.GenericMapStoreBasicIT;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MongoGenericMapStoreBasicIT extends GenericMapStoreBasicIT {

    @BeforeClass
    public static void beforeClass() {
        assumeDockerEnabled();
        initialize(new MongoDatabaseProvider());
    }
}
