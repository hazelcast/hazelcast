package com.hazelcast.config.matcher;

import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.ConfigurationException;

/**
 * This <code>ConfigPatternMatcher</code> supports a simplified wildcard matching.
 * See "Config.md ## Using Wildcard" for details about the syntax options.
 * <p/>
 * In addition the candidates are weighted by the best match. The best result is returned.
 * Throws {@link com.hazelcast.config.ConfigurationException} is multiple configurations are found.
 */
public class MatchingPointConfigPatternMatcher implements ConfigPatternMatcher {

    @Override
    public String matches(Iterable<String> configPatterns, String itemName) throws ConfigurationException {
        String candidate = null;
        String duplicate = null;
        int lastMatchingPoint = -1;
        for (String pattern : configPatterns) {
            int matchingPoint = getMatchingPoint(pattern, itemName);
            if (matchingPoint > -1 && matchingPoint >= lastMatchingPoint) {
                if (matchingPoint == lastMatchingPoint) {
                    duplicate = candidate;
                } else {
                    duplicate = null;
                }
                lastMatchingPoint = matchingPoint;
                candidate = pattern;
            }
        }
        if (duplicate != null) {
            throw new ConfigurationException(itemName, candidate, duplicate);
        }
        return candidate;
    }

    /**
     * This method returns higher values the better the matching is.
     *
     * @param pattern  configuration pattern to match with
     * @param itemName item name to match
     * @return -1 if name does not match at all, zero or positive otherwise
     */
    private int getMatchingPoint(String pattern, String itemName) {
        int index = pattern.indexOf('*');
        if (index == -1) {
            return -1;
        }

        String firstPart = pattern.substring(0, index);
        if (!itemName.startsWith(firstPart)) {
            return -1;
        }

        String secondPart = pattern.substring(index + 1);
        if (!itemName.endsWith(secondPart)) {
            return -1;
        }

        return firstPart.length() + secondPart.length();
    }
}
