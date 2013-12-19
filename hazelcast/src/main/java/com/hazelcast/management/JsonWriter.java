package com.hazelcast.management;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: sancar
 * Date: 18/12/13
 * Time: 11:55
 */

public class JsonWriter {
    private StringBuilder jsonString = new StringBuilder();

    public void write(Object o) {
        if (o instanceof JsonWritable) {
            jsonString.append("{");
            ((JsonWritable) o).toJson(this);
            replaceLastIfCommaWith('}');
        } else if (o instanceof String) {
            jsonString.append("\"").append(o.toString()).append("\"");
        } else if (o instanceof AtomicLong) {
            jsonString.append(((AtomicLong) o).get());
        } else {
            jsonString.append(o.toString());
        }
    }

    public void write(String name, Object o) {
        if(o == null) return;
        jsonString.append("\"").append(name).append("\"").append(":");
        write(o);
        jsonString.append(',');
    }

    public void write(String name, AtomicLong o) {
        if(o == null) return;
        jsonString.append("\"").append(name).append("\"").append(":");
        jsonString.append("{\"value\":");
        write(o);
        jsonString.append("},");
    }

    public <K, V> void write(String name, Map<K, V> m) {
        if (m == null) return;
        jsonString.append(String.format("\"%s\":{", name));
        for (Map.Entry<K, V> entry : m.entrySet()) {
            write(entry.getKey());
            jsonString.append(':');
            write(entry.getValue());
            jsonString.append(',');
        }
        replaceLastIfCommaWith('}');
        jsonString.append(',');
    }

    public <E> void write(String listName, Collection<E> l) {
        if (l == null) return;
        jsonString.append(String.format("\"%s\":[", listName));
        for (E s : l) {
            write(s);
            jsonString.append(',');
        }
        replaceLastIfCommaWith(']');
        jsonString.append(',');
    }

    private void replaceLastIfCommaWith(char replace) {
        if (jsonString.charAt(jsonString.length() - 1) == ',') {
            jsonString.setCharAt(jsonString.length() - 1, replace);
        } else {
            jsonString.append(replace);
        }
    }

    public String getJsonString() {
        return jsonString.toString();
    }

}
