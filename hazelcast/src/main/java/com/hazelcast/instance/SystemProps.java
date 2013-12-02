package com.hazelcast.instance;

import java.io.InputStream;
import java.util.Properties;

/**
 * User: ahmetmircik
 * Date: 12/2/13
 */
public enum SystemProps {

    INSTANCE;

    private String version;
    private String build;
    private int buildNumber;

    private SystemProps(){
        version = System.getProperty("hazelcast.version", "unknown");
        build = System.getProperty("hazelcast.build", "unknown");
        if ("unknown".equals(version) || "unknown".equals(build)) {
            try {
                final InputStream inRuntimeProperties = NodeInitializer.class.getClassLoader().getResourceAsStream("hazelcast-runtime.properties");
                if (inRuntimeProperties != null) {
                    Properties runtimeProperties = new Properties();
                    runtimeProperties.load(inRuntimeProperties);
                    inRuntimeProperties.close();
                    version = runtimeProperties.getProperty("hazelcast.version");
                    build = runtimeProperties.getProperty("hazelcast.build");
                }
            } catch (Exception ignored) {
            }
        }
        try {
            buildNumber = Integer.getInteger("hazelcast.build", -1);
            if (buildNumber == -1) {
                buildNumber = Integer.parseInt(build);
            }
        } catch (Exception ignored) {
        }
    }

    public String getVersion() {
        return version;
    }

    public String getBuild() {
        return build;
    }

    public int getBuildNumber() {
        return buildNumber;
    }
}
