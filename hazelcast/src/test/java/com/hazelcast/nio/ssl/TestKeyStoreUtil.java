/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.ExceptionUtil.rethrow;

@SuppressWarnings({"WeakerAccess", "unused"})
public final class TestKeyStoreUtil {

    public static final String JAVAX_NET_SSL_KEY_STORE = "javax.net.ssl.keyStore";
    public static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    public static final String JAVAX_NET_SSL_KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    public static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";
    public static final String JAVAX_NET_SSL_MUTUAL_AUTHENTICATION = "javax.net.ssl.mutualAuthentication";
    public static final String JAVAX_NET_SSL_KEY_FILE = "javax.net.ssl.keyFile";
    public static final String JAVAX_NET_SSL_KEY_CERT_CHAIN_FILE = "javax.net.ssl.keyCertChainFile";
    public static final String JAVAX_NET_SSL_TRUST_CERT_COLLECTION_FILE = "javax.net.ssl.trustCertCollectionFile";
    public static final String KEY_FILE = "keyFile";

    private static final ILogger LOGGER = Logger.getLogger(TestKeyStoreUtil.class.getName());

    private static String keyStore;
    private static String trustStore;
    private static String wrongKeyStore;
    private static String malformedKeystore;
    private static String keyFile;
    private static String certFile;

    private TestKeyStoreUtil() {
    }

    public static Properties createSslProperties() {
        return createSslProperties(false);
    }

    public static Properties createSslProperties(boolean openSsl) {
        Properties props = new Properties();
        if (openSsl) {
            props.setProperty(JAVAX_NET_SSL_KEY_FILE, getKeyFilePath());
            props.setProperty(JAVAX_NET_SSL_KEY_CERT_CHAIN_FILE, getCertFilePath());
            props.setProperty(JAVAX_NET_SSL_TRUST_CERT_COLLECTION_FILE, getCertFilePath());
        } else {
            props.setProperty(JAVAX_NET_SSL_KEY_STORE, getKeyStoreFilePath());
            props.setProperty(JAVAX_NET_SSL_TRUST_STORE, getTrustStoreFilePath());
            props.setProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, "123456");
            props.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, "123456");
        }
        return props;
    }

    public static synchronized String getKeyStoreFilePath() {
        if (keyStore == null || !new File(keyStore).exists()) {
            keyStore = createTempFile("com/hazelcast/nio/ssl/hazelcast.keystore").getAbsolutePath();
        }
        return keyStore;
    }

    public static synchronized String getKeyFilePath() {
        if (keyFile == null || !new File(keyFile).exists()) {
            keyFile = createTempFile("com/hazelcast/nio/ssl/hazelcast-privkey.pem").getAbsolutePath();
        }
        return keyFile;
    }

    public static synchronized String getCertFilePath() {
        if (certFile == null || !new File(certFile).exists()) {
            certFile = createTempFile("com/hazelcast/nio/ssl/hazelcast.crt").getAbsolutePath();
        }
        return certFile;
    }

    public static synchronized String getTrustStoreFilePath() {
        if (trustStore == null || !new File(trustStore).exists()) {
            trustStore = createTempFile("com/hazelcast/nio/ssl/hazelcast.truststore").getAbsolutePath();
        }
        return trustStore;
    }

    public static synchronized String getWrongKeyStoreFilePath() {
        if (wrongKeyStore == null || !new File(wrongKeyStore).exists()) {
            wrongKeyStore = createTempFile("com/hazelcast/nio/ssl/hazelcast_wrong.keystore").getAbsolutePath();
        }
        return wrongKeyStore;
    }

    public static synchronized String getMalformedKeyStoreFilePath() {
        if (malformedKeystore == null || !new File(malformedKeystore).exists()) {
            malformedKeystore = createTempFile("com/hazelcast/nio/ssl/hazelcast_malformed.keystore").getAbsolutePath();
        }
        return malformedKeystore;
    }

    private static File createTempFile(String resource) {
        InputStream in = null;
        OutputStream out = null;
        try {
            File file = File.createTempFile("hazelcast", "jks");
            file.deleteOnExit();

            ClassLoader cl = TestKeyStoreUtil.class.getClassLoader();
            in = new BufferedInputStream(cl.getResourceAsStream(resource));
            out = new BufferedOutputStream(new FileOutputStream(file));
            int b;
            while ((b = in.read()) > -1) {
                out.write(b);
            }
            out.flush();

            LOGGER.warning("Keystore file path: " + file.getAbsolutePath() + ", length: " + file.length());
            return file;
        } catch (IOException e) {
            throw rethrow(e);
        } finally {
            closeResource(out);
            closeResource(in);
        }
    }
}
