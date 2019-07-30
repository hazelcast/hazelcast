package com.hazelcast.client.impl.protocol.util;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class PropertiesUtil {
    public static Properties fromMap(Map<String, String> map) {
        Properties properties = new Properties();
        properties.putAll(map);
        return properties;
    }

    public static Map<String, String> toMap(Properties properties) {
        return properties.entrySet().stream().collect(
                Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue().toString()
                )
        );
    }
}
