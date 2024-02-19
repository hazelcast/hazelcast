/*
 * Copyright 2024 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kafka.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestUtil {

    static Pattern mongoIndexExtraction = Pattern.compile(".*numberLong\": \"(\\d*).*");

    private static final ObjectMapper MAPPER = new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

    private TestUtil() {
    }

    static String convertToString(SourceRecord rec) {
        return Values.convertToString(rec.valueSchema(), rec.value());
    }

    static <T> T convertToType(SourceRecord rec, Class<T> clazz) {
        try {
            return MAPPER.readValue(Values.convertToString(rec.valueSchema(), rec.value()), clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static String convertToStringWithJustIndexForMongo(SourceRecord rec) {
        String stringValue = Values.convertToString(rec.valueSchema(), rec.value());
        Matcher matcher = mongoIndexExtraction.matcher(stringValue);
        if (matcher.find()) {
            return String.format("%04d", Integer.parseInt(matcher.group(1)));
        } else {
            return "Not found in :" + stringValue;
        }
    }

    static URL getConnectorURL(String fileName) {
        ClassLoader classLoader = TestUtil.class.getClassLoader();
        URL resource = classLoader.getResource(fileName);
        assert resource != null;
        try {
            assertThat(new File(resource.toURI())).exists();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return resource;
    }
}
