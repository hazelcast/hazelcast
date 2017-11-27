/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.ssl;

import java.net.InetAddress;
import java.security.cert.Certificate;
import java.util.Properties;

import javax.net.ssl.SSLPeerUnverifiedException;

/**
 * Interface for additional host validation in SSL/TLS connections. The implementing classes have to provide a public no-args
 * constructor.
 */
public interface TlsHostVerifier {

    /**
     * Inits the verifier instance after creation with properties.
     *
     * @param properties verifier properties (may be {@code null})
     */
    void init(Properties properties);

    /**
     * Verifies the given host address and its list of {@link Certificate}s are valid combination for this
     * {@link TlsHostVerifier} implementation. If the verification fails, the {@link SSLPeerUnverifiedException} is thrown.
     *
     * @param hostAddress Peer address.
     * @param hostCertificates Peer certificates. May be {@code null} if checking is enabled on the server side of TLS connection
     *        and a TLS client comes without authentication.
     * @throws SSLPeerUnverifiedException if the host was not verified.
     */
    void verifyHost(InetAddress hostAddress, Certificate[] hostCertificates) throws SSLPeerUnverifiedException;
}
