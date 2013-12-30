package com.hazelcast.util;

public final class JvmUtil {

    private static final String ORACLE_VENDOR_STRING = "Oracle";
    private static final String SUN_VENDOR_STRING = "Sun";
    private static final String IBM_VENDOR_STRING = "IBM";

    private static final Vendor VENDOR;
    private static final Version VERSION;

    static {
        VENDOR = findJvmVendor();
        VERSION = findJvmVersion();
    }

    private JvmUtil() {
    }

    public static Vendor getJvmVendor() {
        return VENDOR;
    }

    public static Version getJvmVersion() {
        return VERSION;
    }

    private static Vendor findJvmVendor() {
        String vendorString = System.getProperty("java.vendor");
        if (vendorString.contains(ORACLE_VENDOR_STRING)) {
            return Vendor.SunOracle;
        } else if (vendorString.contains(SUN_VENDOR_STRING)) {
            return Vendor.SunOracle;
        } else if (vendorString.contains(IBM_VENDOR_STRING)) {
            return Vendor.IBM;
        }
        return Vendor.Unknown;
    }

    private static Version findJvmVersion() {
        String versionString = System.getProperty("java.version");
        if (versionString.startsWith("1.6")) {
            return Version.Java6;
        } else if (versionString.startsWith("1.7")) {
            return Version.Java7;
        } else if (versionString.startsWith("1.8")) {
            return Version.Java8;
        }
        return Version.Unknown;
    }

    public static enum Version {
        Java6,
        Java7,
        Java8,
        Unknown
    }

    public static enum Vendor {
        SunOracle,
        IBM,
        Unknown
    }

}
