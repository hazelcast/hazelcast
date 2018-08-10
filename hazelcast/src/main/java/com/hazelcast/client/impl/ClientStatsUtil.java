package com.hazelcast.client.impl;

import static com.hazelcast.util.StringUtil.isNullOrEmptyAfterTrim;
import static java.util.Arrays.asList;
import static java.util.regex.Pattern.quote;

import java.util.List;
import java.util.Map;

/**
 * Moved from MC to OSS to deal with client statistics as map(s) on members
 * instead of just a {@link String} (so that statistics themselves are
 * integrated in new metrics system).
 */
final class ClientStatsUtil {

    private static final char STAT_SEPARATOR = ',';
    private static final char KEY_VALUE_SEPARATOR = '=';
    static final char STAT_NAME_PART_SEPARATOR = '.';

    private ClientStatsUtil() {
    }

    static Long getLongOrNull(Map<String, String> statMap, String statName) {
        String statText = statMap.get(statName);
        if (isNullOrEmptyAfterTrim(statText)) {
            return null;
        }
        return Long.valueOf(statText);
    }

    static Double getDoubleOrNull(Map<String, String> statMap, String statName) {
        String statText = statMap.get(statName);
        if (isNullOrEmptyAfterTrim(statText)) {
            return null;
        }
        return Double.valueOf(statText);
    }

    static Integer getIntegerOrNull(Map<String, String> statMap, String statName) {
        String statText = statMap.get(statName);
        if (isNullOrEmptyAfterTrim(statText)) {
            return null;
        }
        return Integer.valueOf(statText);
    }

    /**
     * @param s the string for which the escape character '\' is removed properly
     * @return the unescaped string
     */
    static String unescapeSpecialCharacters(String s) {
        return s.replaceAll("\\\\(.)", "$1");
    }

    /**
     * This method uses ',' character by default. It is for splitting into key=value tokens.
     *
     * @param statString the statistics string to be split
     * @return a list of split strings
     */
    static List<String> splitStats(String statString) {
        return split(statString, STAT_SEPARATOR);
    }

    /**
     * This method uses '.' character by default. It is for splitting a stat name into parts.
     *
     * @param statName the statistics name to be split
     * @return a list of split strings
     */
    static List<String> splitStatName(String statName) {
        return split(statName, STAT_NAME_PART_SEPARATOR);
    }

    /**
     * This method uses '=' character by default. It is for splitting a key-value string into a key-value pair.
     *
     * @param keyValuePair the key-value string to be split
     * @return a list of key and value
     */
    static List<String> splitKeyValuePair(String keyValuePair) {
        return split(keyValuePair, KEY_VALUE_SEPARATOR);
    }

    /**
     * @param stat      statistics string to be split
     * @param splitChar A special character to be used for split, e.g. '='
     * @return a list of split strings
     */
    private static List<String> split(String stat, char splitChar) {
        return stat.length() == 0 ? null : asList(stat.split("(?<!\\\\)" + quote("" + splitChar)));
    }
}
