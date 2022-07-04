/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.kubernetes;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.kubernetes.KubernetesConfig.ExposeExternallyMode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.kubernetes.KubernetesConfig.DiscoveryMode;
import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_API_RETIRES;
import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_API_TOKEN;
import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_CA_CERTIFICATE;
import static com.hazelcast.kubernetes.KubernetesProperties.NAMESPACE;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_DNS;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_DNS_TIMEOUT;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_LABEL_NAME;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_LABEL_VALUE;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_NAME;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KubernetesConfigTest {
    private static final String TEST_API_TOKEN = "api-token";
    private static final String TEST_CA_CERTIFICATE = "ca-certificate";
    private static final String TEST_NAMESPACE = "test";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void dnsLookupMode() {
        // given
        String serviceDns = "hazelcast.default.svc.cluster.local";
        int serviceDnsTimeout = 10;
        int servicePort = 5703;

        Map<String, Comparable> properties = createProperties();
        properties.put(SERVICE_DNS.key(), serviceDns);
        properties.put(SERVICE_DNS_TIMEOUT.key(), serviceDnsTimeout);
        properties.put(SERVICE_PORT.key(), servicePort);

        // when
        KubernetesConfig config = new KubernetesConfig(properties);

        // then
        assertEquals(DiscoveryMode.DNS_LOOKUP, config.getMode());
        assertEquals(serviceDns, config.getServiceDns());
        assertEquals(serviceDnsTimeout, config.getServiceDnsTimeout());
        assertEquals(servicePort, config.getServicePort());
    }

    @Test
    public void dnsLookupModeWithoutServiceAccountToken() {
        // given
        String serviceDns = "hazelcast.default.svc.cluster.local";
        int serviceDnsTimeout = 10;
        int servicePort = 5703;

        Map<String, Comparable> properties = new HashMap<>();
        properties.put(SERVICE_DNS.key(), serviceDns);
        properties.put(SERVICE_DNS_TIMEOUT.key(), serviceDnsTimeout);
        properties.put(SERVICE_PORT.key(), servicePort);

        // when
        KubernetesConfig config = new KubernetesConfig(properties);

        // then
        assertEquals(DiscoveryMode.DNS_LOOKUP, config.getMode());
        assertEquals(serviceDns, config.getServiceDns());
        assertEquals(serviceDnsTimeout, config.getServiceDnsTimeout());
        assertEquals(servicePort, config.getServicePort());
        assertNull(config.getKubernetesCaCertificate());
    }

    @Test
    public void kubernetesApiModeDefault() throws Exception {
        // given
        Map<String, Comparable> properties = createProperties();

        // when
        KubernetesConfig config = new KubernetesConfig(properties);

        // then
        assertEquals(DiscoveryMode.KUBERNETES_API, config.getMode());
        assertEquals("test", config.getNamespace());
        assertEquals(true, config.isResolveNotReadyAddresses());
        assertEquals(false, config.isUseNodeNameAsExternalAddress());
        assertEquals(TEST_API_TOKEN, config.getTokenProvider().getToken());
        assertEquals(TEST_CA_CERTIFICATE, config.getKubernetesCaCertificate());
        assertEquals(ExposeExternallyMode.AUTO, config.getExposeExternallyMode());
    }

    @Test
    public void kubernetesApiModeServiceName() {
        // given
        String serviceName = "service-name";
        Map<String, Comparable> properties = createProperties();
        properties.put(SERVICE_NAME.key(), serviceName);

        // when
        KubernetesConfig config = new KubernetesConfig(properties);

        // then
        assertEquals(DiscoveryMode.KUBERNETES_API, config.getMode());
        assertEquals(serviceName, config.getServiceName());
    }

    @Test
    public void kubernetesApiModeServiceLabel() {
        // given
        String serviceLabelName = "service-label-name";
        String serviceLabelValue = "service-label-value";
        Map<String, Comparable> properties = createProperties();
        properties.put(KubernetesProperties.SERVICE_LABEL_NAME.key(), serviceLabelName);
        properties.put(SERVICE_LABEL_VALUE.key(), serviceLabelValue);

        // when
        KubernetesConfig config = new KubernetesConfig(properties);

        // then
        assertEquals(DiscoveryMode.KUBERNETES_API, config.getMode());
        assertEquals(serviceLabelName, config.getServiceLabelName());
        assertEquals(serviceLabelValue, config.getServiceLabelValue());
    }

    @Test
    public void kubernetesApiNodeNameAsExternalAddress() {
        // given
        Map<String, Comparable> properties = createProperties();
        properties.put(KubernetesProperties.USE_NODE_NAME_AS_EXTERNAL_ADDRESS.key(), true);

        // when
        KubernetesConfig config = new KubernetesConfig(properties);

        // then
        assertEquals(true, config.isUseNodeNameAsExternalAddress());
    }

    @Test
    public void kubernetesApiExposeExternally() {
        // given
        Map<String, Comparable> properties = createProperties();
        properties.put(KubernetesProperties.EXPOSE_EXTERNALLY.key(), true);

        // when
        KubernetesConfig config = new KubernetesConfig(properties);

        // then
        assertEquals(ExposeExternallyMode.ENABLED, config.getExposeExternallyMode());
    }

    @Test
    public void readTokenCertificateAndNamespaceFromFilesWhenPropertiesNotSet() throws Exception {
        // given
        KubernetesConfig.FileContentsReader dummyFileContentsReader = fileName -> {
            switch (fileName) {
                case "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt":
                    return "certificate-xyz";
                case "/var/run/secrets/kubernetes.io/serviceaccount/namespace":
                    return "namespace-xyz";
                default:
                    throw new RuntimeException("File not found");
            }
        };
        // when
        KubernetesConfig config = new KubernetesConfig(Collections.emptyMap(), dummyFileContentsReader);

        // then
        assertEquals("certificate-xyz", config.getKubernetesCaCertificate());
        assertEquals("namespace-xyz", config.getNamespace());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void invalidConfigurationBothModesConfigured() {
        // given
        Map<String, Comparable> properties = createProperties();
        properties.put(SERVICE_NAME.key(), "service-name");
        properties.put(SERVICE_DNS.key(), "service-dns");

        // when
        new KubernetesConfig(properties);

        // then
        // throws exception
    }

    @Test(expected = InvalidConfigurationException.class)
    public void invalidConfigurationBothModesConfiguredServiceLabel() {
        // given
        Map<String, Comparable> properties = createProperties();
        properties.put(SERVICE_LABEL_NAME.key(), "service-label-name");
        properties.put(SERVICE_LABEL_VALUE.key(), "service-label-value");
        properties.put(SERVICE_DNS.key(), "service-dns");

        // when
        new KubernetesConfig(properties);

        // then
        // throws exception
    }

    @Test(expected = InvalidConfigurationException.class)
    public void invalidConfigurationBothServiceNameAndLabel() {
        // given
        Map<String, Comparable> properties = createProperties();
        properties.put(SERVICE_NAME.key(), "service-name");
        properties.put(SERVICE_LABEL_NAME.key(), "service-label-name");
        properties.put(SERVICE_LABEL_VALUE.key(), "service-label-value");

        // when
        new KubernetesConfig(properties);

        // then
        // throws exception
    }

    @Test(expected = InvalidConfigurationException.class)
    public void invalidServiceDnsTimeout() {
        // given
        Map<String, Comparable> properties = createProperties();
        properties.put(SERVICE_DNS.key(), "service-dns");
        properties.put(SERVICE_DNS_TIMEOUT.key(), -1);

        // when
        new KubernetesConfig(properties);

        // then
        // throws exception
    }

    @Test(expected = InvalidConfigurationException.class)
    public void invalidKubernetesApiRetries() {
        // given
        Map<String, Comparable> properties = createProperties();
        properties.put(KUBERNETES_API_RETIRES.key(), -1);

        // when
        new KubernetesConfig(properties);

        // then
        // throws exception
    }

    @Test(expected = InvalidConfigurationException.class)
    public void invalidServicePort() {
        // given
        Map<String, Comparable> properties = createProperties();
        properties.put(SERVICE_PORT.key(), -1);

        // when
        new KubernetesConfig(properties);

        // then
        // throws exception
    }

    private static Map<String, Comparable> createProperties() {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        // Predefined test properties
        properties.put(KUBERNETES_API_TOKEN.key(), TEST_API_TOKEN);
        properties.put(KUBERNETES_CA_CERTIFICATE.key(), TEST_CA_CERTIFICATE);
        properties.put(NAMESPACE.key(), TEST_NAMESPACE);
        return properties;
    }

    @Test
    public void DefaultFileContentsReader_readFileContents()
            throws IOException {
        String expectedContents = "Hello, world!\nThis is a test with Unicode ✓.";
        String testFile = createTestFile(expectedContents);
        String actualContents = new KubernetesConfig.DefaultFileContentsReader().readFileContents(testFile);
        assertEquals(expectedContents, actualContents);
    }

    private String createTestFile(String content)
            throws IOException {
        File file = tempFolder.newFile("test.tmp");
        return Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8)).toString();
    }

    @Test
    public void propertyServiceNameIsEmpty() {
        // given
        Map<String, Comparable> properties = createProperties();
        properties.put(SERVICE_NAME.key(), "  ");
        String serviceDns = "service-dns";
        properties.put(SERVICE_DNS.key(), serviceDns);

        //when
        KubernetesConfig config = new KubernetesConfig(properties);

        //then
        assertEquals(serviceDns, config.getServiceDns());

    }

    @Test
    public void propertyServiceDnsIsNull() {
        // given
        Map<String, Comparable> properties = createProperties();
        String serviceName = "service-name";
        properties.put(SERVICE_NAME.key(), serviceName);
        properties.put(SERVICE_DNS.key(), null);

        //when
        KubernetesConfig config = new KubernetesConfig(properties);

        //then
        assertEquals(serviceName, config.getServiceName());

    }

    @Test
    public void emptyProperties() {
        // given
        Map<String, Comparable> properties = createProperties();
        properties.put(SERVICE_LABEL_NAME.key(), "  ");
        String serviceLabelValue = "service-label-value";
        properties.put(SERVICE_LABEL_VALUE.key(), serviceLabelValue);
        properties.put(SERVICE_DNS.key(), "");

        //when
        KubernetesConfig config = new KubernetesConfig(properties);

        //then
        assertEquals(serviceLabelValue, config.getServiceLabelValue());
    }
}
