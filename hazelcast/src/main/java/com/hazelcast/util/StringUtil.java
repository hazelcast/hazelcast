package com.hazelcast.util;

import java.util.Locale;

/**
 * User: ahmetmircik
 * Date: 11/14/13
 */
public final class StringUtil {

    private static final Locale LOCALE_INTERNAL = Locale.ENGLISH;

    private StringUtil() {
    }

    /**
     *
     * HC specific settings, operands etc. use this method.
     *
     * */
    public static String upperCaseInternal(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        return str.toUpperCase(LOCALE_INTERNAL);
    }

    /**
     *
     * HC specific settings, operands etc. use this method.
     *
     * */
    public static String lowerCaseInternal(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        return str.toLowerCase(LOCALE_INTERNAL);
    }

}
