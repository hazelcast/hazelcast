package com.hazelcast.internal.tpcengine.net;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.SocketAddress;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Properties;

public class DefaultSSLEngineFactory implements SSLEngineFactory {
    private static String keyStoreFile = "testkeys";
    private static String trustStoreFile = "testkeys";
    private static String passwd = "passphrase";

    private SSLContext sslContext;

    public DefaultSSLEngineFactory() {
        try {
            KeyManager[] keyManagers = createKeyManagers("/home/pveentjer/server.jks", "storepass", "keypass");

            TrustManager[] trustManagers = createTrustManagers("/home/pveentjer/trustedCerts.jks", "storepass");

            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagers, trustManagers, new SecureRandom());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void init(Properties properties, boolean forClient) throws Exception {

    }

    @Override
    public SSLEngine create(boolean clientMode, SocketAddress peerAddress) {
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(clientMode);
        return sslEngine;
    }

    /**
     * Creates the key managers required to initiate the {@link SSLContext}, using a JKS keystore as an input.
     *
     * @param filepath         - the path to the JKS keystore.
     * @param keystorePassword - the keystore's password.
     * @param keyPassword      - the key's passsword.
     * @return {@link KeyManager} array that will be used to initiate the {@link SSLContext}.
     * @throws Exception
     */
    protected KeyManager[] createKeyManagers(String filepath, String keystorePassword, String keyPassword) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        InputStream keyStoreIS = new FileInputStream(filepath);
        try {
            keyStore.load(keyStoreIS, keystorePassword.toCharArray());
        } finally {
            if (keyStoreIS != null) {
                keyStoreIS.close();
            }
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword.toCharArray());
        return kmf.getKeyManagers();
    }

    /**
     * Creates the trust managers required to initiate the {@link SSLContext}, using a JKS keystore as an input.
     *
     * @param filepath         - the path to the JKS keystore.
     * @param keystorePassword - the keystore's password.
     * @return {@link TrustManager} array, that will be used to initiate the {@link SSLContext}.
     * @throws Exception
     */
    protected TrustManager[] createTrustManagers(String filepath, String keystorePassword) throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        InputStream trustStoreIS = new FileInputStream(filepath);
        try {
            trustStore.load(trustStoreIS, keystorePassword.toCharArray());
        } finally {
            if (trustStoreIS != null) {
                trustStoreIS.close();
            }
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory.getTrustManagers();
    }
}
