/*
 * Copyright 2023 Hazelcast Inc.
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
package com.hazelcast.jet.mongodb.dataconnection;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, IgnoreInJenkinsOnWindows.class})
public class MongoDataConnectionMutualSslTest extends MongoDataConnectionSslTestBase {

    @Override
    protected String getConfigurationFile() {
        return "tlsMongoMutual.yaml";
    }

    @Override
    protected DataConnectionConfig getDataConnectionConfig() {
        return new DataConnectionConfig("mongoDB")
                .setType("Mongo")
                .setName("mongoDB")
                .setProperty("database", DATABASE)
                .setProperty("enableSsl", "true")
                .setProperty("invalidHostNameAllowed", "true")
                .setProperty("connectionString", connectionString)
                .setProperty("trustStore", trustStoreLocation)
                .setProperty("trustStoreType", "pkcs12")
                .setProperty("trustStorePassword", "123456")
                .setProperty("keyStore", keyStoreLocation)
                .setProperty("keyStoreType", "pkcs12")
                .setProperty("keyStorePassword", "123456")
                .setShared(true);
    }

    @Nonnull
    @Override
    protected SSLContext getSslContext() {
        return SslConf.createSSLContext(keyStoreLocation, "pkcs12", "123456".toCharArray(),
                trustStoreLocation, "pkcs12", "123456".toCharArray());
    }

}
