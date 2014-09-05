/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.io.*;
import java.util.Properties;

/**
 * @author mdogan 9/6/13
 */

public class TestKeyStoreUtil {

    public static final String JAVAX_NET_SSL_KEY_STORE = "javax.net.ssl.keyStore";
    public static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    public static final String JAVAX_NET_SSL_KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    public static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

    private static String keyStore;
    private static String trustStore;

    private TestKeyStoreUtil() {
    }

    public static synchronized String getKeyStoreFilePath() throws IOException {
        if (keyStore == null || !new File(keyStore).exists()) {
            keyStore = createTempKeyStoreFile("com/hazelcast/nio/ssl/hazelcast.keystore").getAbsolutePath();
        }
        return keyStore;
    }

    public static synchronized String getTrustStoreFilePath() throws IOException {
        if (trustStore == null || !new File(trustStore).exists()) {
            trustStore = createTempKeyStoreFile("com/hazelcast/nio/ssl/hazelcast.truststore").getAbsolutePath();
        }
        return trustStore;
    }

    private static File createTempKeyStoreFile(String resource) throws IOException {
        ClassLoader cl = TestKeyStoreUtil.class.getClassLoader();
        InputStream in = new BufferedInputStream(cl.getResourceAsStream(resource));
        File file = File.createTempFile("hazelcast", "jks");
        OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
        int b;
        while ((b = in.read()) > -1) {
            out.write(b);
        }
        out.flush();
        out.close();
        in.close();
        file.deleteOnExit();
        return file;
    }

    public static Properties createSslProperties() throws IOException {
        Properties props = new Properties();
        props.setProperty(JAVAX_NET_SSL_KEY_STORE, getKeyStoreFilePath());
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE, getTrustStoreFilePath());
        props.setProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, "123456");
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, "123456");
        return props;
    }

}
