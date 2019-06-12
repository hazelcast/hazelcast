package com.hazelcast.internal.util;

import com.hazelcast.logging.ILogger;

import java.security.Provider;
import java.security.Security;

public class CipherUtils {

    private CipherUtils() {
    }

    public static void initBouncySecurityProvider(ILogger logger) {
        try {
            if (Boolean.getBoolean("hazelcast.security.bouncy.enabled")) {
                String provider = "org.bouncycastle.jce.provider.BouncyCastleProvider";
                Security.addProvider((Provider) Class.forName(provider).newInstance());
            }
        } catch (Exception e) {
            if (logger == null) {
                throw new RuntimeException(e);
            } else {
                logger.warning(e);
            }
        }
    }

}
