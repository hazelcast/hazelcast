/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.UsernamePasswordIdentityConfig;
import com.hazelcast.internal.dynamicconfig.DynamicConfigGeneratorUtil;
import com.hazelcast.internal.dynamicconfig.DynamicConfigYamlGenerator;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Config outputs mask sensitive values: the Management Center view does,
 * while exports, dynamic configuration persistence and client config round-trips
 * never do because they are parsed back as configuration.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SensitiveConfigOutputTest {

    private static final String LICENSE = "LICENSE-SECRET-1234567890";
    private static final String DATA_CONNECTION_URL = "jdbc:postgresql://db.example/app?password=URL_SECRET";
    private static final List<String> SECRETS = List.of("LICENSE-SECRET", "SSL_KS_SECRET", "SSL_TS_SECRET",
            "IDENTITY_PW_SECRET", "AWS_AK_SECRET", "AWS_SK_SECRET", "GCE_CRED_SECRET", "URL_SECRET", "DC_PW_SECRET",
            "MAPSTORE_SECRET");

    @Test
    public void managementCenterView_masksSensitiveValues() {
        String xml = new ConfigXmlGenerator(true, true).generate(configWithSecrets());

        assertThat(SECRETS).noneMatch(xml::contains);
        assertThat(xml).contains("jdbc:postgresql://db.example/app?password=****");
    }

    @Test
    public void unmaskedXml_keepsSensitiveValues() {
        String xml = new ConfigXmlGenerator(true, false).generate(configWithSecrets());

        assertThat(SECRETS).allMatch(xml::contains);
    }

    @Test
    public void yamlExport_keepsSensitiveValues() {
        String yaml = new DynamicConfigYamlGenerator().generate(configWithSecrets(), false);

        assertThat(List.of("LICENSE-SECRET", "SSL_KS_SECRET", "SSL_TS_SECRET", "AWS_AK_SECRET", "AWS_SK_SECRET",
                "GCE_CRED_SECRET", "URL_SECRET", "DC_PW_SECRET", "MAPSTORE_SECRET")).allMatch(yaml::contains);
    }

    @Test
    public void dynamicConfigPersistence_keepsSensitiveValues() {
        Config config = configWithSecrets();
        DataConnectionConfig dataConnection = config.getDataConnectionConfig("pg");
        MapConfig map = config.getMapConfig("m");

        for (boolean xml : new boolean[]{true, false}) {
            assertThat(DynamicConfigGeneratorUtil.dataConnectionConfigGenerator(dataConnection, xml, 2))
                    .contains("DC_PW_SECRET", "URL_SECRET");
            assertThat(DynamicConfigGeneratorUtil.mapConfigGenerator(map, xml, 2)).contains("MAPSTORE_SECRET");
            assertThat(DynamicConfigGeneratorUtil.licenseKeyConfigGenerator(LICENSE, xml, 2)).contains(LICENSE);
        }
    }

    @Test
    public void clientConfigRoundTrip_keepsSensitiveValues() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true)
                .setProperty("keyStorePassword", "SSL_KS_SECRET"));
        clientConfig.getNetworkConfig().getAwsConfig().setEnabled(true)
                .setProperty("secret-key", "AWS_SK_SECRET");

        ClientConfig parsed = ImdgUtil.asClientConfig(ImdgUtil.asXmlString(clientConfig));

        assertEquals("SSL_KS_SECRET", parsed.getNetworkConfig().getSSLConfig().getProperty("keyStorePassword"));
        assertEquals("AWS_SK_SECRET", parsed.getNetworkConfig().getAwsConfig().getProperty("secret-key"));
    }

    private static Config configWithSecrets() {
        Config config = new Config();
        config.setLicenseKey(LICENSE);
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true)
                .setProperty("keyStorePassword", "SSL_KS_SECRET")
                .setProperty("trustStorePassword", "SSL_TS_SECRET"));
        config.getNetworkConfig().getJoin().getAwsConfig().setEnabled(true)
                .setProperty("access-key", "AWS_AK_SECRET")
                .setProperty("secret-key", "AWS_SK_SECRET");
        config.getNetworkConfig().getJoin().getDiscoveryConfig().addDiscoveryStrategyConfig(
                new DiscoveryStrategyConfig("com.hazelcast.jclouds.JCloudsDiscoveryStrategy")
                        .addProperty("credential", "GCE_CRED_SECRET"));
        config.addDataConnectionConfig(new DataConnectionConfig("pg").setType("jdbc")
                .setProperty("jdbcUrl", DATA_CONNECTION_URL)
                .setProperty("password", "DC_PW_SECRET"));
        config.getMapConfig("m").getMapStoreConfig().setEnabled(true).setClassName("com.example.Store")
                .setProperty("password", "MAPSTORE_SECRET");
        config.getSecurityConfig().setEnabled(true).setMemberRealmConfig("realm",
                new RealmConfig().setUsernamePasswordIdentityConfig(
                        new UsernamePasswordIdentityConfig("user", "IDENTITY_PW_SECRET")));
        return config;
    }
}
