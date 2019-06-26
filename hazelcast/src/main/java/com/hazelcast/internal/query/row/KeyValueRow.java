package com.hazelcast.internal.query.row;

/**
 * Key-value row. Appears during iteration over a data stored in map or it's index.
 */
public class KeyValueRow implements Row {
    /** Extractor. */
    private final KeyValueRowExtractor extractor;

    /** Key. */
    private Object key;

    /** Value. */
    private Object val;

    public KeyValueRow(KeyValueRowExtractor extractor) {
        this.extractor = extractor;
    }

    public void setKeyValue(Object key, Object val) {
        this.key = key;
        this.val = val;
    }

    @Override
    public Object getColumn(int idx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnCount() {
        throw new UnsupportedOperationException();
    }

    /**
     * Extract the value with the given path.
     *
     * @param path Path.
     * @return Extracted value.
     */
    public Object extract(String path) {
        return extractor.extract(key, val, path);
    }
}
