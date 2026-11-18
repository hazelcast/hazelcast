/*
 * Copyright 2026 Hazelcast Inc.
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

package com.hazelcast.jet.pulsar;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.SerialTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.cert.CertPathBuilderException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.pulsarDataConnectionConf;
import static com.hazelcast.jet.pulsar.PulsarSchema.string;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.PulsarContainer.BROKER_HTTP_PORT;
import static org.testcontainers.containers.PulsarContainer.BROKER_PORT;

@SerialTest
@ParameterizedClass
@MethodSource("com.hazelcast.jet.pulsar.PulsarTestSupport#pulsarVersions")
@Timeout(value = 120, unit = SECONDS)
class PulsarTlsE2ETest extends JetTestSupport {
    private static final String DATA_CONNECTION = "pulsar-tls";
    private static final int BROKER_TLS_PORT = 6651;
    private static final int HTTPS_PORT = 8443;
    private static PulsarContainer container;

    @Parameter
    static DockerImageName imageName;

    @BeforeParameterizedClassInvocation
    static void startContainer(DockerImageName image) {
        container = new TlsPulsarContainer(image)
                .withExposedPorts(BROKER_PORT, BROKER_HTTP_PORT, BROKER_TLS_PORT, HTTPS_PORT)
                .withCopyFileToContainer(MountableFile.forClasspathResource("tls/broker-cert.pem"), "/pulsar/tls/cert.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("tls/broker-key.pem",
                        Transferable.DEFAULT_FILE_MODE), "/pulsar/tls/key.pem")
                .withEnv("PULSAR_PREFIX_brokerServicePortTls", Integer.toString(BROKER_TLS_PORT))
                .withEnv("PULSAR_PREFIX_webServicePortTls", Integer.toString(HTTPS_PORT))
                .withEnv("PULSAR_PREFIX_tlsCertificateFilePath", "/pulsar/tls/cert.pem")
                .withEnv("PULSAR_PREFIX_tlsKeyFilePath", "/pulsar/tls/key.pem")
                .withEnv("PULSAR_PREFIX_tlsTrustCertsFilePath", "/pulsar/tls/cert.pem")
                .withEnv("PULSAR_PREFIX_internalListenerName", "internal")
                .withEnv("PULSAR_LOG_LEVEL", "ERROR");
        container.start();
    }

    @AfterParameterizedClassInvocation
    static void stopContainer() {
        if (container != null) {
            container.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sourceAndSinkTransferMessagesOverTls(boolean useConsumer) throws Exception {
        DataConnectionConfig connectionConfig = connectionConfig("tls/broker-cert.pem");
        Config config = smallInstanceConfig().addDataConnectionConfig(connectionConfig);
        var member = createHazelcastInstances(config, 2)[0];
        String inputTopic = "persistent://public/default/" + randomName();
        String outputTopic = "persistent://public/default/" + randomName();
        List<String> expected = IntStream.range(0, 100).mapToObj(i -> "tls-message-" + i).toList();

        // Exercise the admin HTTPS endpoint through the same data connection as the pipeline.
        PulsarDataConnection connection = new PulsarDataConnection(connectionConfig);
        try (var admin = connection.getAdmin();
             PulsarClient client = connection.getClient()) {
            admin.topics().createPartitionedTopic(inputTopic, 3);
            admin.topics().createPartitionedTopic(outputTopic, 3);
            assertThat(admin.topics().getPartitionedTopicMetadata(inputTopic).partitions).isEqualTo(3);

            StreamSource<String> source = useConsumer
                    ? PulsarSources.pulsarConsumerBuilder(string(), Message::getValue)
                                   .topic(inputTopic)
                                   .dataConnectionRef(dataConnectionRef(DATA_CONNECTION))
                                   .build()
                    : PulsarSources.pulsarReaderBuilder(string())
                                   .topic(inputTopic)
                                   .dataConnectionRef(dataConnectionRef(DATA_CONNECTION))
                                   .build();
            Pipeline pipeline = Pipeline.create();
            pipeline.readFrom(source).withoutTimestamps()
                    .writeTo(PulsarSinks.builder(string())
                                       .topic(outputTopic)
                                       .dataConnectionRef(dataConnectionRef(DATA_CONNECTION))
                                       .extractValueFn(identity())
                                       .build());

            try (var reader = client.newReader(Schema.STRING).topic(outputTopic)
                                    .startMessageId(MessageId.earliest).create();
                 var producer = client.newProducer(Schema.STRING).topic(inputTopic).create()) {
                Job job = member.getJet().newJob(pipeline);
                try {
                    assertThat(job).eventuallyHasStatus(RUNNING);
                    for (String value : expected) {
                        producer.send(value);
                    }
                    List<String> actual = new ArrayList<>();
                    long deadline = System.nanoTime() + SECONDS.toNanos(60);
                    while (actual.size() < expected.size() && System.nanoTime() < deadline) {
                        Message<String> message = reader.readNext(1, SECONDS);
                        if (message != null) {
                            actual.add(message.getValue());
                        }
                    }
                    assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
                } finally {
                    job.cancel();
                }
            }
        } finally {
            connection.destroy();
        }
    }

    @Test
    void clientRejectsUntrustedBrokerCertificate() throws Exception {
        DataConnectionConfig config = connectionConfig("tls/untrusted-cert.pem")
                .setProperty("pulsar.client.lookupTimeoutMs", "3000")
                .setProperty("pulsar.client.operationTimeoutMs", "3000");
        PulsarDataConnection connection = new PulsarDataConnection(config);
        try (PulsarClient client = connection.getClient()) {
            assertThatThrownBy(() -> {
                try (var ignored = client.newProducer(Schema.STRING).topic(randomName()).create()) {
                    // Close the producer if the untrusted certificate is unexpectedly accepted.
                }
            }).isInstanceOf(PulsarClientException.class)
              .hasRootCauseInstanceOf(CertPathBuilderException.class);
        } finally {
            connection.destroy();
        }
    }

    @Test
    void adminRejectsUntrustedBrokerCertificate() throws Exception {
        PulsarDataConnection connection = new PulsarDataConnection(connectionConfig("tls/untrusted-cert.pem"));
        try (var admin = connection.getAdmin()) {
            assertThatThrownBy(() -> admin.tenants().getTenants())
                    .isInstanceOf(PulsarAdminException.class)
                    .hasRootCauseInstanceOf(CertPathBuilderException.class);
        } finally {
            connection.destroy();
        }
    }

    private static DataConnectionConfig connectionConfig(String trustCertificate) throws Exception {
        String certificatePath = Path.of(PulsarTlsE2ETest.class.getClassLoader()
                .getResource(trustCertificate).toURI()).toString();
        return pulsarDataConnectionConf(DATA_CONNECTION,
                "pulsar+ssl://" + container.getHost() + ":" + container.getMappedPort(BROKER_TLS_PORT),
                "https://" + container.getHost() + ":" + container.getMappedPort(HTTPS_PORT))
                .setProperty("pulsar.tlsTrustCertsFilePath", certificatePath)
                .setProperty("pulsar.tlsAllowInsecureConnection", "false")
                .setProperty("pulsar.tlsHostnameVerificationEnable", "true")
                .setProperty("client.listenerName", "external");
    }

    private static final class TlsPulsarContainer extends PulsarContainer {
        private TlsPulsarContainer(DockerImageName image) {
            super(image);
        }

        @Override
        protected void configure() {
            super.configure();
            // Wait for Docker to assign host ports before advertising the TLS listener.
            withCommand("/bin/bash", "-c",
                    "while [ ! -f /tmp/start-pulsar.sh ]; do sleep 0.1; done; exec bash /tmp/start-pulsar.sh");
        }

        @Override
        protected void containerIsStarting(InspectContainerResponse containerInfo) {
            String script = "#!/bin/bash\n"
                    + "export PULSAR_PREFIX_advertisedListeners='internal:pulsar://localhost:6650,"
                    + "external:pulsar+ssl://" + getHost() + ":" + getMappedPort(BROKER_TLS_PORT) + "'\n"
                    + "/pulsar/bin/apply-config-from-env.py /pulsar/conf/standalone.conf\n"
                    + "exec /pulsar/bin/pulsar standalone --no-functions-worker -nss\n";
            copyFileToContainer(Transferable.of(script.getBytes(StandardCharsets.UTF_8)), "/tmp/start-pulsar.sh");
        }
    }
}
