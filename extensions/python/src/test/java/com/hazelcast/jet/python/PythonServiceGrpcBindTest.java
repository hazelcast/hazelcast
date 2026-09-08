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

package com.hazelcast.jet.python;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;

import static com.hazelcast.jet.python.PythonTransforms.mapUsingPythonBatch;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for the port handshake between Jet and the Python gRPC server.
 * <p>
 * {@code jet_to_python_grpc_server.py} binds its gRPC server to an ephemeral port and phones the
 * port number back to {@link JetToPythonServer}, which then connects to {@code 127.0.0.1:<port>}.
 * The port must therefore really belong to the Python gRPC server <em>on 127.0.0.1</em>.
 * <p>
 * grpcio 1.80.0 broke this for {@code "localhost:0"}: it resolves {@code localhost} to both
 * {@code ::1} and {@code 127.0.0.1}, binds {@code [::1]} first and reports success even when the
 * same port could not be bound on {@code 127.0.0.1} because another process already owns it there
 * (typically the phone-back socket of another tasklet). Jet then talked to that other process and
 * the handshake failed with {@code NumberFormatException: For input string: "PRI * HTTP/2.0"}.
 * <p>
 * The kernel picks the ephemeral port, so the collision cannot be forced from the outside. Instead
 * the test ships a handler module which the Python server imports right before it creates the gRPC
 * server; the module wraps {@code grpc.server} so that the {@code :0} bind is redirected to a port
 * the test already occupies on 127.0.0.1 with a decoy socket. A correct Python server must then
 * fail to start. A broken one phones back the decoy's port and Jet connects to the decoy, which
 * records what it received (the HTTP/2 preface {@code PRI * HTTP/2.0}) for the assertion message.
 * <p>
 * The decoy's port is an ephemeral port the kernel picked for 127.0.0.1, so it is practically
 * always free on {@code [::1]}, which the regression needs. On a host without IPv6 loopback a broken
 * server fails to bind both addresses and the test passes without exercising the regression; the
 * test cannot detect that because the surefire JVM runs with {@code -Djava.net.preferIPv4Stack=true}.
 */
@Category({QuickTest.class, ParallelJVMTest.class})
public class PythonServiceGrpcBindTest extends SimpleTestInClusterSupport {

    private static final String PORT_PLACEHOLDER = "@PORT@";

    /**
     * Echo handler that also installs the test hook described in the class Javadoc.
     */
    private static final String PORT_FORCING_ECHO_HANDLER = """
            import grpc

            # TEST HOOK. jet_to_python_grpc_server.py imports this module right before it creates
            # the gRPC server, so wrapping grpc.server here lets the test replace the ephemeral
            # port (":0") with a port that the test already occupies on 127.0.0.1. This simulates
            # the kernel handing out a port on [::1] that another process owns on 127.0.0.1.
            _FORCED_PORT = @PORT@
            _real_grpc_server = grpc.server


            def _port_forcing_grpc_server(*args, **kwargs):
                server = _real_grpc_server(*args, **kwargs)
                real_add_insecure_port = server.add_insecure_port

                def add_insecure_port(address):
                    host, _, port = address.rpartition(':')
                    if port == '0':
                        address = '%s:%d' % (host, _FORCED_PORT)
                    print('TEST HOOK: binding the gRPC server to %s' % address, flush=True)
                    bound_port = real_add_insecure_port(address)
                    print('TEST HOOK: add_insecure_port returned %d' % bound_port, flush=True)
                    return bound_port

                server.add_insecure_port = add_insecure_port
                return server


            grpc.server = _port_forcing_grpc_server


            def handle(input_list):
                return ['echo-%s' % i for i in input_list]
            """;

    private File baseDir;
    private DecoySocket decoy;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, smallInstanceWithResourceUploadConfig());
        assumeThatNoWindowsOS();
    }

    @Before
    public void before() throws Exception {
        baseDir = createTempDirectory();
        decoy = new DecoySocket();
    }

    @After
    public void after() throws Exception {
        if (decoy != null) {
            decoy.close();
        }
        IOUtil.delete(baseDir);
    }

    @Test
    public void when_grpcPortIsTakenOnIpv4Loopback_then_pythonServerMustNotPhoneItBack() throws Exception {
        // Given: the port the Python gRPC server is forced to bind is already owned by the decoy on 127.0.0.1
        installFileToBaseDir(PORT_FORCING_ECHO_HANDLER.replace(PORT_PLACEHOLDER, String.valueOf(decoy.port())),
                "echo.py");
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setBaseDir(baseDir.toString())
                .setHandlerModule("echo")
                .setHandlerFunction("handle");
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items("1", "2", "3"))
                .apply(mapUsingPythonBatch(cfg)).setLocalParallelism(1)
                .writeTo(Sinks.noop());

        // When
        Job job = instance().getJet().newJob(p);
        Throwable jobFailure = catchThrowable(() -> job.getFuture().get(3, MINUTES));

        // Then: nobody may have talked to the decoy ...
        assertThat(decoy.firstLineReceived())
                .as("Jet connected to the decoy socket on 127.0.0.1:%d, so the Python gRPC server phoned back"
                                + " a port it does not own on 127.0.0.1 (grpcio partial-bind regression)."
                                + " The decoy received: %s",
                        decoy.port(), decoy.firstLineReceived())
                .isNull();
        // ... and the Python server must have refused to start because its port is taken
        assertThat(jobFailure)
                .as("the job must fail because the port forced on the Python gRPC server is taken")
                .isNotNull()
                .hasStackTraceContaining("Python process died before completing initialization");
    }

    private void installFileToBaseDir(String contents, String filename) throws IOException {
        try (InputStream in = new ByteArrayInputStream(contents.getBytes(UTF_8))) {
            Files.copy(in, new File(baseDir, filename).toPath());
        }
    }

    /**
     * Plays the role of "another process" owning a port on 127.0.0.1. In production that is the
     * phone-back {@link ServerSocket} of another tasklet's {@link JetToPythonServer}: like that
     * socket, it reads one line from whoever connects and then hangs up. The line is kept for the
     * assertion message.
     */
    private static final class DecoySocket implements AutoCloseable {
        private static final int READ_TIMEOUT_MILLIS = 5_000;

        private final ServerSocket serverSocket;
        private volatile String firstLineReceived;

        DecoySocket() throws IOException {
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            Thread acceptor = new Thread(this::acceptLoop, "python-grpc-bind-test-decoy-" + port());
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        String firstLineReceived() {
            return firstLineReceived;
        }

        private void acceptLoop() {
            while (!serverSocket.isClosed()) {
                try (Socket client = serverSocket.accept()) {
                    client.setSoTimeout(READ_TIMEOUT_MILLIS);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream(), UTF_8));
                    String line = reader.readLine();
                    if (firstLineReceived == null) {
                        firstLineReceived = line != null ? line : "<connection closed without sending a line>";
                    }
                } catch (IOException e) {
                    if (serverSocket.isClosed()) {
                        return;
                    }
                    if (firstLineReceived == null) {
                        firstLineReceived = "<connection accepted, but reading a line failed: " + e + ">";
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
        }
    }
}
