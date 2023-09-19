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

package com.hazelcast.spi.utils;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.internal.util.JavaVersion;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.exception.RestClientException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static com.hazelcast.internal.nio.IOUtil.close;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public class RestClientTest {
    private static final String API_ENDPOINT = "/some/endpoint";
    private static final String BODY_REQUEST = "some body request";
    private static final String BODY_RESPONSE = "some body response";

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    private String address;

    @Before
    public void setUp() {
        address = String.format("http://localhost:%s", wireMockRule.port());
    }

    @Test
    public void getSuccess() {
        // given
        stubFor(get(urlEqualTo(API_ENDPOINT))
            .willReturn(aResponse().withStatus(200).withBody(BODY_RESPONSE)));

        // when
        String result = RestClient.create(String.format("%s%s", address, API_ENDPOINT)).get().getBody();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    @Test
    public void getWithHeadersSuccess() {
        // given
        String headerKey = "Metadata-Flavor";
        String headerValue = "Google";
        stubFor(get(urlEqualTo(API_ENDPOINT))
            .withHeader(headerKey, equalTo(headerValue))
            .willReturn(aResponse().withStatus(200).withBody(BODY_RESPONSE)));

        // when
        String result = RestClient.create(String.format("%s%s", address, API_ENDPOINT))
            .withHeaders(singletonMap(headerKey, headerValue))
            .get()
            .getBody();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    @Test
    public void getWithRetries() {
        // given
        stubFor(get(urlEqualTo(API_ENDPOINT))
            .inScenario("Retry Scenario")
            .whenScenarioStateIs(STARTED)
            .willReturn(aResponse().withStatus(500).withBody("Internal error"))
            .willSetStateTo("Second Try"));
        stubFor(get(urlEqualTo(API_ENDPOINT))
            .inScenario("Retry Scenario")
            .whenScenarioStateIs("Second Try")
            .willReturn(aResponse().withStatus(200).withBody(BODY_RESPONSE)));

        // when
        String result = RestClient.create(String.format("%s%s", address, API_ENDPOINT))
            .withReadTimeoutSeconds(1200)
            .withConnectTimeoutSeconds(1200)
            .withRetries(1)
            .get()
            .getBody();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    @Test(expected = Exception.class)
    public void getFailure() {
        // given
        stubFor(get(urlEqualTo(API_ENDPOINT))
            .willReturn(aResponse().withStatus(500).withBody("Internal error")));

        // when
        RestClient.create(String.format("%s%s", address, API_ENDPOINT)).get();

        // then
        // throw exception
    }

    @Test
    public void postSuccess() {
        // given
        stubFor(post(urlEqualTo(API_ENDPOINT))
            .withRequestBody(equalTo(BODY_REQUEST))
            .willReturn(aResponse().withStatus(200).withBody(BODY_RESPONSE)));

        // when
        String result = RestClient.create(String.format("%s%s", address, API_ENDPOINT))
            .withBody(BODY_REQUEST)
            .post()
            .getBody();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    @Test
    public void expectedResponseCode() {
        // given
        int expectedCode1 = 201;
        int expectedCode2 = 202;
        stubFor(post(urlEqualTo(API_ENDPOINT))
                .withRequestBody(equalTo(BODY_REQUEST))
                .willReturn(aResponse().withStatus(expectedCode1).withBody(BODY_RESPONSE)));

        // when
        String result = RestClient.create(String.format("%s%s", address, API_ENDPOINT))
                .withBody(BODY_REQUEST)
                .expectResponseCodes(expectedCode1, expectedCode2)
                .post()
                .getBody();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    @Test
    public void expectHttpOkByDefault() {
        // given
        int responseCode = 201;
        stubFor(post(urlEqualTo(API_ENDPOINT))
                .withRequestBody(equalTo(BODY_REQUEST))
                .willReturn(aResponse().withStatus(responseCode).withBody(BODY_RESPONSE)));

        // when
        RestClientException exception = assertThrows(RestClientException.class, () ->
                RestClient.create(String.format("%s%s", address, API_ENDPOINT))
                        .withBody(BODY_REQUEST)
                        .get());

        // then
        assertEquals(exception.getHttpErrorCode(), responseCode);
    }

    @Test
    public void unexpectedResponseCode() {
        // given
        int expectedCode = 201;
        int unexpectedCode = 202;
        stubFor(post(urlEqualTo(API_ENDPOINT))
                .withRequestBody(equalTo(BODY_REQUEST))
                .willReturn(aResponse().withStatus(unexpectedCode).withBody(BODY_RESPONSE)));

        // when
        RestClientException exception = assertThrows(RestClientException.class, () ->
                RestClient.create(String.format("%s%s", address, API_ENDPOINT))
                        .withBody(BODY_REQUEST)
                        .expectResponseCodes(expectedCode)
                        .post());

        // then
        assertEquals(exception.getHttpErrorCode(), unexpectedCode);
    }

    @Test
    public void readErrorResponse() {
        // given
        int responseCode = 418;
        String responseMessage = "I'm a teapot";
        stubFor(post(urlEqualTo(API_ENDPOINT))
                .withRequestBody(equalTo(BODY_REQUEST))
                .willReturn(aResponse().withStatus(responseCode).withBody(responseMessage)));

        // when
        RestClient.Response response = RestClient.create(String.format("%s%s", address, API_ENDPOINT))
                .withBody(BODY_REQUEST)
                .expectResponseCodes(responseCode)
                .get();

        // then
        assertEquals(responseCode, response.getCode());
        assertEquals(responseMessage, response.getBody());
    }

    @Test
    public void tls13SupportDefaultCacert() throws IOException {
        assumeTrue(JavaVersion.isAtLeast(JavaVersion.JAVA_11));
        Tls13CipherCheckingServer server = new Tls13CipherCheckingServer(new ServerSocket(0));
        try {
            new Thread(server).start();
            RestClient.create("https://127.0.0.1:" + server.serverSocket.getLocalPort()).get();
        } catch (Exception e) {
            // whatever
        } finally {
            server.shutdownRequested = true;
        }
        assertTrue("No TLS 1.3 cipher used", server.tls13CipherFound.get());
    }

    @Test
    public void tls13SupportCustomCacert() throws IOException {
        assumeTrue(JavaVersion.isAtLeast(JavaVersion.JAVA_11));
        Tls13CipherCheckingServer server = new Tls13CipherCheckingServer(new ServerSocket(0));
        try {
            new Thread(server).start();
            RestClient.create("https://127.0.0.1:" + server.serverSocket.getLocalPort())
                    .withCaCertificates(readFile("src/test/resources/kubernetes/ca.crt")).get();
        } catch (Exception e) {
            // whatever
        } finally {
            server.shutdownRequested = true;
        }
        assertTrue("No TLS 1.3 cipher used", server.tls13CipherFound.get());
    }

    private String readFile(String fileName) throws IOException {
        return StringUtil.bytesToString(Files.readAllBytes(Paths.get(fileName)));
    }

    static final class Tls13CipherCheckingServer implements Runnable {
        private static final ILogger LOGGER = Logger.getLogger(Tls13CipherCheckingServer.class);

        final ServerSocket serverSocket;
        volatile boolean shutdownRequested;
        final AtomicBoolean tls13CipherFound = new AtomicBoolean();

        Tls13CipherCheckingServer(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
            try {
                this.serverSocket.setSoTimeout(500);
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("The server will be listening on port " + serverSocket.getLocalPort());
        }

        public void run() {
            try {
                while (!(shutdownRequested || tls13CipherFound.get())) {
                    try {
                        Socket socket = serverSocket.accept();
                        new Thread(() -> {
                            LOGGER.info("Socket accepted " + socket);
                            try {
                                socket.setSoTimeout(5000);
                                tls13CipherFound.compareAndSet(false, hasTls13Cipher(socket.getInputStream()));
                            } catch (IOException e) {
                                LOGGER.warning("Reading from the socket failed", e);
                            } finally {
                                close(socket);
                            }
                        }).start();
                    } catch (SocketTimeoutException e) {
                        // it's fine
                    }
                }
            } catch (IOException e) {
                LOGGER.warning("The test server thrown an exception", e);
            } finally {
                close(serverSocket);
            }
        }

        void stop() {
            shutdownRequested = true;
        }

        static boolean hasTls13Cipher(InputStream is) throws IOException {
            try (DataInputStream dis = new DataInputStream(is)) {
                int type = dis.readUnsignedByte();
                // HANDSHAKE record
                if (type != 0x16) {
                    throw new IOException("Not a handshake record");
                }
                // skip protocol version
                skip(dis, 2);
                // skip record size
                dis.readUnsignedShort();
                int msgType = dis.readUnsignedByte();
                if (msgType != 0x01) {
                    throw new IOException("Not a ClientHello message");
                }
                // skip length
                skip(dis, 3);
                // skip client version
                skip(dis, 2);
                // skip client random
                skip(dis, 32);
                // skip sessionId
                skip(dis, dis.readUnsignedByte());
                // cipher suites
                int cipherCount = dis.readUnsignedShort() / 2;
                // check if at least one of TLS 1.3 mandatory cipher suites is present
                for (int i = 0; i < cipherCount; i++) {
                    int b1 = dis.readUnsignedByte();
                    int b2 = dis.readUnsignedByte();
                    // https://datatracker.ietf.org/doc/html/rfc8446#appendix-B.4
                    if (b1 == 0x13 && b2 >= 1 && b2 <= 3) {
                        return true;
                    }
                }
            }
            return false;
        }

        private static void skip(DataInputStream dis, int toSkip) throws IOException {
            for (int i = 0; i < toSkip; i++) {
                dis.readByte();
            }
        }
    }

}
