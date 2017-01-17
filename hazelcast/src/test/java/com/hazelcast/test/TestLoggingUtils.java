package com.hazelcast.test;

public final class TestLoggingUtils {
    private static String LOGGING_TYPE_PROP_NAME = "hazelcast.logging.type";
    private static String LOGGING_CLASS_PROP_NAME = "hazelcast.logging.class";

    private TestLoggingUtils() {
    }

    public static void initializeLogging() {
        if (shouldForceTestLoggingFactory()) {
            System.setProperty(LOGGING_CLASS_PROP_NAME, TestLoggerFactory.class.getName());
            System.setProperty("isThreadContextMapInheritable", "true");
            System.clearProperty(LOGGING_TYPE_PROP_NAME);
        }
    }

    private static boolean shouldForceTestLoggingFactory() {
        if (JenkinsDetector.isOnJenkins()) {
            return true;
        }
        if (System.getProperty(LOGGING_TYPE_PROP_NAME) == null && System.getProperty(LOGGING_CLASS_PROP_NAME) == null) {
            return true;
        }
        return false;
    }
}
