package com.hazelcast.internal.probing;

/**
 * Contains utilities to work with {@link CharSequence} instead of
 * {@link String} mainly to avoid creating intermediate representations that
 * would lead to garbage objects.
 */
public final class CharSequenceUtils {

    private CharSequenceUtils() {
        // utility
    }

    /**
     * Escapes a user-supplied name.
     * 
     * Effectively adds a backslash before comma ({@code ","}), space ({@code " "}),
     * equals sign ({@code "="}) and backslash ({@code "\"}).
     */
    static void appendEscaped(StringBuilder buf, CharSequence name) {
        int len = name.length();
        for (int i = 0; i < len; i++) {
            char c = name.charAt(i);
            if (c == ',' || c == ' ' || c == '\\' || c == '=') {
                buf.append('\\');
            }
            buf.append(c);
        }
    }

    /**
     * Removes backslash escaping of line feeds for a for a user supplied name (as
     * received from client where line-feeds need extra level of escaping when all
     * metrics are transported as a {@link String}).
     */
    static void appendUnescaped(StringBuilder buf, CharSequence name) {
        int len = name.length();
        for (int i = 0; i < len; i++) {
            char c = name.charAt(i);
            if (c != '\\' || i+1 >= len || name.charAt(i+1) != '\n') {
                buf.append(c);
            }
        }
    }

    /**
     * Check if a {@link CharSequence} starts with a given prefix.
     * 
     * This helps to avoid allocation of intermediate {@link String} objects to
     * perform {@link String#startsWith(String)} that would be required otherwise.
     * 
     * @param prefix not null
     * @param s search string, not null
     * @return true, if s starts with prefix, else false
     */
    public static boolean startsWith(CharSequence prefix, CharSequence s) {
        int len = prefix.length();
        if (len > s.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (prefix.charAt(i) != s.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Parses a long value from the complete sequence of a given
     * {@link CharSequence}.
     * 
     * This helps to avoid allocation when parsing longs as {@link Long}s utility
     * methods unnecessarily require a {@link String} to be passed what would cause
     * intermediate garbage objects.
     * 
     * Implementation is inspired by {@link Long#parseLong(String)}.
     * 
     * @param s a sequence giving a long number
     * @return the number as {@code long}
     * @throws NumberFormatException in case the sequence contain non digit
     *         characters
     */
    public static long parseLong(CharSequence s) throws NumberFormatException {
        int len = s.length();
        if (len == 0) {
            return 0L;
        }
        char firstChar = s.charAt(0);
        int i = 0;
        boolean negative = false;
        long res = 0L;
        if (firstChar < '0') {
            negative = firstChar == '-';
            i++;
        }
        while (i < len) {
            int digit = s.charAt(i++) - '0';
            if (digit < 0 || digit > 9) {
                throw new NumberFormatException("For input string: \"" + s + "\"");
            }
            res *= 10;
            // Accumulating negatively avoids surprises near MAX_VALUE
            res -= digit;
        }
        return negative ? res : -res;
    }

    /**
     * Allows to split a wrapped {@link CharSequence} in lines without creating
     * intermediate objects.
     * 
     * The {@link Lines} instance therefore becomes a {@link CharSequence}
     * representing one line at a time.
     * 
     * The {@link #next()} method is used to forward to the next line.
     * {@link #key()} can be used to go backwards to end of previous key (expected
     * to be called at a line end representing a key value pair separated by space)
     * {@link #value()} can be used to read the value after the current position
     * without changing the sequence this represents.
     */
    static final class Lines implements CharSequence {

        private final CharSequence str;
        private final int len;
        private int start;
        private int end;

        public Lines(CharSequence str) {
            this.str = str;
            this.len = str.length();
            this.start = 0;
            this.end = -1;
        }

        Lines next() {
            start = end + 1;
            end = nextLineFeed();
            return this;
        }

        private int nextLineFeed() {
            int i = start;
            char c = ' ';
            while (i < len && c != '\n') {
                c = str.charAt(i++);
                if (c == '\\') {
                    i++; // skip escaped char
                }
            }
            return i - 1;
        }

        /**
         * @return Backs {@link #end} to previous space and returns this
         */
        CharSequence key() {
            int i = end-1;
            while (i >= 0 && str.charAt(i) != ' ') {
                i--;
            }
            end = i;
            return this;
        }

        /**
         * @return returns the value expected after current {@link #end}.
         */
        long value() {
            int s = start;
            int e = end;
            start = end + 1;
            end = nextLineFeed();
            long val = parseLong(this);
            start = s;
            end = e;
            return val;
        }

        @Override
        public int length() {
            return end <= start ? 0 : end - start;
        }

        @Override
        public char charAt(int index) {
            return str.charAt(start + index);
        }

        @Override
        public Lines subSequence(int start, int end) {
            this.start += start;
            this.end = this.start + (end - start);
            return this;
        }

        @Override
        public String toString() {
            return new StringBuilder(this).toString();
        }
    }

}
