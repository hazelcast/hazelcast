package com.hazelcast.internal.util.phonehome;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Util class for parameters of OS and EE PhoneHome pings.
 */
public class PhoneHomeParameterCreator {

    private final StringBuilder builder;
    private final Map<String, String> parameters = new HashMap<>();
    private boolean hasParameterBefore;

    public PhoneHomeParameterCreator() {
        builder = new StringBuilder();
        builder.append("?");
    }

    Map<String, String> getParameters() {
        return parameters;
    }

    public PhoneHomeParameterCreator addParam(String key, String value) {

        if (hasParameterBefore) {
            builder.append("&");
        } else {
            hasParameterBefore = true;
        }
        try {
            builder.append(key).append("=").append(URLEncoder.encode(value, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw rethrow(e);
        }
        parameters.put(key, value);
        return this;
    }

    String build() {
        return builder.toString();
    }
}

