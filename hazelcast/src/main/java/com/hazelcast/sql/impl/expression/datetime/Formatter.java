/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.sql.impl.QueryException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

import java.time.DayOfWeek;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.JulianFields;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalField;
import static java.time.temporal.WeekFields.ISO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of <a href="https://www.postgresql.org/docs/14/functions-formatting.html">
 * PostgreSQL <code>to_char()</code></a> with the following differences.
 * <ol><li> {@code FM} and {@code TM} prefixes enable the fill and translation modes for date
 *          patterns respectively, which are disabled by default.
 *     <li> {@code RY}/{@code ry} and {@code RD}/{@code rd} patterns are introduced to format
 *          {@link ChronoField#YEAR_OF_ERA} and {@link ChronoField#DAY_OF_MONTH} respectively as
 *          roman numerals.
 *     <li> {@code V} pattern can be combined with a decimal point.
 *     <li> {@code M} and {@code P} patterns are introduced as the anchored versions of {@code MI}
 *          and {@code PL} patterns respectively.
 *     <li> {@code PR} pattern is replaced with {@code B} and {@code BR} patterns. The former is
 *          the anchored version of the latter.
 *     <li> {@code L} pattern is replaced with {@code C} and {@code CR} patterns. The former is the
 *          anchored version of the latter.
 *     <li> Literals are anchored by default. To fix its position, a literal should be prepended
 *          with an {@code F} pattern.
 *     <li> Zero-padding and space-padding are completely orthogonal, which makes it possible to
 *          have zero-padded fractions, which are aligned at the the decimal separator. However,
 *          this necessitates to have a {@code '0'} at the end of the pattern if the PostgreSQL
 *          convention is desired. </ol>
 */
@SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ExecutableStatementCount", "checkstyle:MagicNumber",
        "checkstyle:MethodLength", "checkstyle:NestedIfDepth", "checkstyle:NPathComplexity"})
public abstract class Formatter {
    private static final String ESCAPED = "\\\\.";
    private static final String LITERAL = "\"(?:" + ESCAPED + "|[^\"])*\"?|" + ESCAPED;
    private static final String DATETIME
            = "SSSSS?|HH(?:12|24)?|MI|[SMU]S|FF[1-6]|[AP](?:M|\\.M\\.)|"
            + "DA?Y|Da?y|I?DDD|DD|I?D|J|W|[WI]W|MON(?:TH)?|Mon(?:th)?|MM|Y,YYY|[YI]Y{0,3}|"
            + "Q|CC|BC|B\\.C\\.|AD|A\\.D\\.|R[DMY]|r[dmy]|TZ[HM]?|TZ|OF";
    private static final String NUMERIC
            = "[,G.D]|FM|BR?|SG?|MI?|PL?|CR?|V9+|TH|EEEE|RN";
    private static final Pattern DATETIME_TEMPLATE = Pattern.compile(
            "((?:FM|fm|TM|tm)*)(" + DATETIME + "|" + DATETIME.toLowerCase() + ")(TH|th)?|" + LITERAL);
    private static final Pattern NUMERIC_TEMPLATE = Pattern.compile(
            "[90]+|" + NUMERIC + "|" + NUMERIC.toLowerCase() + "|[Ff]?" + LITERAL);
    private static final Pattern SIGN = Pattern.compile("[+-]");

    private static final int[] ARABIC = {1000,  900, 500,  400, 100,   90,  50,   40,  10,    9,   5,    4,   1};
    private static final String[] ROMAN = {"M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"};
    private static final String[] ORDINAL = {"th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th"};

    public static Formatter forDates(String format) {
        return new DateFormat(format);
    }
    public static Formatter forNumbers(String format) {
        return new NumberFormat(format);
    }

    public abstract String format(Object input, Locale locale);

    interface GroupProcessor {
        void acceptLiteral(String literal);
        void acceptGroup(String group, Matcher m);
    }

    private static void parse(Pattern template, GroupProcessor processor, String format) {
        Matcher m = template.matcher(format);
        StringBuilder literal = new StringBuilder();
        int i = 0;
        for (; m.find(); i = m.end()) {
            if (m.start() > i) {
                literal.append(format, i, m.start());
            }
            String group = m.group();
            if (group.startsWith("\\")) {
                literal.append(group);
            } else {
                if (literal.length() > 0) {
                    processor.acceptLiteral(literal.toString());
                    literal.setLength(0);
                }
                processor.acceptGroup(group, m);
            }
        }
        if (i < m.regionEnd() || literal.length() > 0) {
            processor.acceptLiteral(literal + format.substring(i));
        }
    }

    private static <T extends Enum<T>> T valueOf(Class<T> type, String name) {
        try {
            return Enum.valueOf(type, name);
        } catch (IllegalArgumentException e) {
            return Enum.valueOf(type, name.toUpperCase());
        }
    }

    /** Expects {@code <LITERAL>|"<LITERAL>"?} where {@code LITERAL: (\.|[^"])*} */
    private static String unescape(String input) {
        StringBuilder s = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c != '"') {
                if (c == '\\' && ++i < input.length()) {
                    c = input.charAt(i);
                }
                s.append(c);
            }
        }
        return s.toString();
    }

    private static String toRoman(int number) {
        StringBuilder s = new StringBuilder(15);
        if (number > 3999) {
            for (int i = 0; i < 15; i++) {
                s.append('#');
            }
        } else {
            for (int i = 0; i < ARABIC.length; i++) {
                for (; number >= ARABIC[i]; number -= ARABIC[i]) {
                    s.append(ROMAN[i]);
                }
            }
        }
        return s.toString();
    }

    private static String getOrdinal(String number) {
        return number.endsWith("11") || number.endsWith("12") || number.endsWith("13")
                ? "th" : ORDINAL[number.charAt(number.length() - 1) - '0'];
    }

    private static class DateFormat extends Formatter implements GroupProcessor {
        static final DateTimeFormatter MERIDIEM_FORMATTER = DateTimeFormatter.ofPattern("a");
        static final DateTimeFormatter TIMEZONE_FORMATTER = DateTimeFormatter.ofPattern("O");
        static final DateTimeFormatter ERA_FORMATTER = DateTimeFormatter.ofPattern("G");

        static final BiFunction<Object, Locale, Object> UPPERCASE = (o, l) -> ((String) o).toUpperCase(l);
        static final BiFunction<Object, Locale, Object> LOWERCASE = (o, l) -> ((String) o).toLowerCase(l);
        static final BiFunction<Object, Locale, Object> WITH_PERIODS = (o, l) -> {
            String r = (String) o;
            return r.length() != 2 ? r : r.charAt(0) + "." + r.charAt(1) + ".";
        };

        private final List<Part> parts = new ArrayList<>();

        interface Part {
            void format(StringBuilder s, Temporal input, Locale locale);
        }

        static class Literal implements Part {
            final String contents;

            Literal(String contents) {
                this.contents = unescape(contents);
            }

            @Override
            public void format(StringBuilder s, Temporal input, Locale locale) {
                s.append(contents);
            }

            @Override
            public String toString() {
                return '"' + contents + '"';
            }
        }

        static class PatternInstance implements Part {
            final boolean fill;
            final boolean translate;
            final Pattern pattern;
            final Ordinal ordinal;

            PatternInstance(boolean fill, boolean translate, Pattern pattern, Ordinal ordinal) {
                this.fill = fill;
                this.translate = translate;
                this.pattern = pattern;
                this.ordinal = ordinal;
            }

            @Override
            public void format(StringBuilder s, Temporal input, Locale locale) {
                Object result = pattern.query.apply(input, translate ? locale : Locale.US);
                String r = result.toString();
                if (pattern == Pattern.TZH) {
                    s.append((int) result < 0 ? '-' : '+');
                    r = (int) result < 0 ? r.substring(1) : r;
                }
                if (fill) {
                    char pad = result instanceof Number ? '0' : ' ';
                    for (int i = r.length(); i < pattern.maxLength; i++) {
                        s.append(pad);
                    }
                }
                s.append(r);
                if (pattern == Pattern.Y_YYY && (fill || r.length() == 4)) {
                    s.insert(s.length() - 3, ',');
                }
                if (ordinal != null) {
                    String th = getOrdinal(r);
                    s.append(ordinal == Ordinal.TH ? th.toUpperCase() : th);
                }
            }

            @Override
            public String toString() {
                return (fill ? "FM" : "") + (translate ? "TM" : "") + pattern + (ordinal == null ? "" : ordinal);
            }
        }

        @SuppressWarnings("checkstyle:MethodParamPad")
        enum Pattern {
            HH12  (ChronoField.CLOCK_HOUR_OF_AMPM, 2), HH(HH12),
            HH24  (ChronoField.HOUR_OF_DAY, 2),
            MI    (ChronoField.MINUTE_OF_HOUR, 2),
            SS    (ChronoField.SECOND_OF_MINUTE, 2),
            MS    (ChronoField.MILLI_OF_SECOND, 3),
            US    (ChronoField.MICRO_OF_SECOND, 6),
            FF1   (MS, (r, l) -> (int) r / 100, 1),
            FF2   (MS, (r, l) -> (int) r / 10, 2),
            FF3   (MS),
            FF4   (US, (r, l) -> (int) r / 100, 4),
            FF5   (US, (r, l) -> (int) r / 10, 5),
            FF6   (US),
            SSSS  (ChronoField.SECOND_OF_DAY, 5), SSSSS (SSSS),
            AM    ((t, l) -> MERIDIEM_FORMATTER.withLocale(l).format(t), 2), PM(AM),
            am    (AM, LOWERCASE), pm(am),
            A_M_  (AM, WITH_PERIODS, 4), P_M_(A_M_),
            a_m_  (am, WITH_PERIODS), p_m_(a_m_),
            YYYY  (ChronoField.YEAR_OF_ERA, 4), Y_YYY(YYYY),
            YYY   (YYYY, (r, l) -> (int) r % 1000, 3),
            YY    (YYYY, (r, l) -> (int) r % 100, 2),
            Y     (YYYY, (r, l) -> (int) r % 10, 1),
            IYYY  (ISO.weekBasedYear(), 4),
            IYY   (IYYY, (r, l) -> (int) r % 1000, 3),
            IY    (IYYY, (r, l) -> (int) r % 100, 2),
            I     (IYYY, (r, l) -> (int) r % 10, 1),
            BC    ((t, l) -> ERA_FORMATTER.withLocale(l).format(t), 2), AD(BC),
            bc    (BC, LOWERCASE), ad(bc),
            B_C_  (BC, WITH_PERIODS, 4), A_D_(B_C_),
            b_c_  (bc, WITH_PERIODS), a_d_(b_c_),
            Month ((t, l) -> java.time.Month.from(t).getDisplayName(TextStyle.FULL, l), 9),
            MONTH (Month, UPPERCASE),
            month (Month, LOWERCASE),
            Mon   ((t, l) -> java.time.Month.from(t).getDisplayName(TextStyle.SHORT, l), 3),
            MON   (Mon, UPPERCASE),
            mon   (Mon, LOWERCASE),
            MM    (ChronoField.MONTH_OF_YEAR, 2),
            Day   ((t, l) -> DayOfWeek.from(t).getDisplayName(TextStyle.FULL, l), 9),
            DAY   (Day, UPPERCASE),
            day   (Day, LOWERCASE),
            Dy    ((t, l) -> DayOfWeek.from(t).getDisplayName(TextStyle.SHORT, l), 3),
            DY    (Dy, UPPERCASE),
            dy    (Dy, LOWERCASE),
            DDD   (ChronoField.DAY_OF_YEAR, 3),
            IDDD  ((t, l) -> (t.get(ISO.weekOfWeekBasedYear()) - 1) * 7 + t.get(ISO.dayOfWeek()), 3),
            DD    (ChronoField.DAY_OF_MONTH, 2),
            D     (ChronoField.DAY_OF_WEEK, 1),
            ID    (ISO.dayOfWeek(), 1),
            W     (ChronoField.ALIGNED_WEEK_OF_MONTH, 1),
            WW    (ChronoField.ALIGNED_WEEK_OF_YEAR, 2),
            IW    (ISO.weekOfWeekBasedYear(), 2),
            CC    ((t, l) -> (int) Math.ceil(t.get(ChronoField.YEAR_OF_ERA) / 100f), 2),
            J     ((t, l) -> t.getLong(JulianFields.JULIAN_DAY), 7),
            Q     (IsoFields.QUARTER_OF_YEAR, 1),
            RY    ((t, l) -> toRoman(t.get(ChronoField.YEAR_OF_ERA)), 15),
            ry    (RY, LOWERCASE),
            RM    ((t, l) -> toRoman(t.get(ChronoField.MONTH_OF_YEAR)), 4),
            rm    (RM, LOWERCASE),
            RD    ((t, l) -> toRoman(t.get(ChronoField.DAY_OF_MONTH)), 6),
            rd    (RD, LOWERCASE),
            TZ    ((t, l) -> SIGN.split(TIMEZONE_FORMATTER.withLocale(l).format(t))[0], 3),
            tz    (TZ, LOWERCASE),
            TZH   ((t, l) -> ZoneOffset.from(t).getTotalSeconds() / 3600, 2),
            TZM   ((t, l) -> (ZoneOffset.from(t).getTotalSeconds() % 3600) / 60, 2),
            OF    ((t, l) -> ZoneOffset.from(t).getId(), 9);

            final BiFunction<Temporal, Locale, Object> query;
            final int maxLength;

            Pattern(Pattern pattern) {
                this(pattern.query, pattern.maxLength);
            }
            Pattern(TemporalField field, int maxLength) {
                this((t, l) -> t.get(field), maxLength);
            }
            Pattern(Pattern pattern, BiFunction<Object, Locale, Object> transform) {
                this(pattern, transform, pattern.maxLength);
            }
            Pattern(Pattern pattern, BiFunction<Object, Locale, Object> transform, int maxLength) {
                this((t, l) -> transform.apply(pattern.query.apply(t, l), l), maxLength);
            }
            Pattern(BiFunction<Temporal, Locale, Object> query, int maxLength) {
                this.query = query;
                this.maxLength = maxLength;
            }
        }

        enum Ordinal { TH, th }

        DateFormat(String format) {
            parse(DATETIME_TEMPLATE, this, format);
        }

        @Override
        public void acceptLiteral(String literal) {
            parts.add(new Literal(literal));
        }

        @Override
        public void acceptGroup(String group, Matcher m) {
            if (group.startsWith("\"")) {
                parts.add(new Literal(group));
            } else {
                String prefix = m.group(1).toUpperCase();
                String suffix = m.group(3);
                String pattern = m.group(2).replace('.', '_').replace(',', '_');
                parts.add(new PatternInstance(prefix.contains("FM"), prefix.contains("TM"),
                        valueOf(Pattern.class, pattern), suffix == null ? null : Ordinal.valueOf(suffix)));
            }
        }

        @Override
        public String format(Object input, Locale locale) {
            if (!(input instanceof Temporal)) {
                throw QueryException.dataException("Input parameter is expected to be date/time");
            }
            StringBuilder s = new StringBuilder();
            parts.forEach(p -> p.format(s, (Temporal) input, locale));
            return s.toString();
        }

        @Override
        public String toString() {
            return parts.toString();
        }
    }

    /**
     * This implementation does not leverage {@link DecimalFormat} or {@link java.util.Formatter}
     * because of the following reasons.
     * <ol><li> {@link DecimalFormat} does not fill the pattern space with #'s when the integer
     *          part overflows. Instead, it truncates the integer part, which cannot be checked
     *          without obtaining its decimal representation.
     *     <li> {@link DecimalFormat} does not support multiple grouping sizes and separators.
     *     <li> {@link DecimalFormat} rounds the fractional part using its binary representation,
     *          which produces wrong results even for simple cases such as (0.15 → #.# → 0.1) in
     *          {@link RoundingMode#HALF_UP}.
     *     <li> {@link java.util.Formatter} does not cache or expose the intermediate
     *          representation of the format string.
     *     <li> {@link java.util.Formatter} can correctly handle rounding by leveraging
     *          {@link sun.misc.FormattedFloatingDecimal} for {@link Float} and {@link Double}, and
     *          by using {@link BigDecimal#BigDecimal(BigInteger, int, MathContext)
     *          BigDecimal(BigInteger, scale, precision)}. Both {@link sun.misc.FormattedFloatingDecimal}
     *          and {@link BigDecimal} are using the decimal representation of floating point
     *          numbers, but for the former, the rounding mode is not configurable. </ol>
     */
    private static class NumberFormat extends Formatter {
        private final Form form;
        private final Mask integerMask = new Mask(true);
        private final Mask fractionMask = new Mask(false);
        private final boolean currency;
        private final int shift;

        enum Form { Normal, Exponential, Roman }

        interface Anchorable {
            boolean isAnchored();
        }

        enum Pattern implements Anchorable {
            BR, B, SG, S, MI, M, PL, P, CR, C, TH, th, EEEE, eeee, RN, rn;

            static final List<Pattern> SIGN = Arrays.asList(BR, B, SG, S, MI, M, PL, P);
            static final List<Pattern> ANCHORED = Arrays.asList(B, S, M, P, C, TH, th, EEEE, eeee, RN, rn);

            boolean isSign() {
                return SIGN.contains(this);
            }
            char getSign(boolean pre, boolean negative) {
                if (this == BR || this == B) {
                    return negative ? (pre ? '<' : '>') : ' ';
                } else if (this == SG || this == S) {
                    return negative ? '-' : '+';
                } else if (this == MI || this == M) {
                    return negative ? '-' : ' ';
                } else if (this == PL || this == P) {
                    return negative ? ' ' : '+';
                }
                throw new IllegalArgumentException();
            }

            @Override
            public boolean isAnchored() {
                return ANCHORED.contains(this);
            }
        }

        static class Literal implements Anchorable {
            final String contents;
            final boolean anchored;

            Literal(String literal) {
                anchored = "Ff".indexOf(literal.charAt(0)) == -1;
                contents = unescape(literal.substring(anchored ? 0 : 1));
            }

            @Override
            public boolean isAnchored() {
                return anchored;
            }

            @Override
            public String toString() {
                return (anchored ? "" : "F") + '"' + contents + '"';
            }
        }

        /**
         * Stores {@link Literal}, {@link Pattern}, {@link Character} and {@link Integer} to
         * represent arbitrary text, sign/currency/ordinals/exponent/roman numerals, grouping/
         * decimal separators, and digit groups respectively.
         */
        class Mask {
            final boolean pre;
            final StringBuilder digitMask = new StringBuilder();
            final List<Object> groups = new ArrayList<>();
            final List<Boolean> fillModes = new ArrayList<>();
            boolean fillMode = true;
            int offerSign = -1;
            int digits;
            int minDigits;
            boolean sign;
            Pattern bracket;

            Mask(boolean pre) {
                this.pre = pre;
            }

            void toggleFillMode() {
                fillMode = !fillMode;
            }

            public void add(Object g) {
                groups.add(g);
                fillModes.add(fillMode);
            }

            void addDigits(String digits) {
                digitMask.append(digits);
                add(digits.length());
            }

            void prepare() {
                if (pre) {
                    Collections.reverse(groups);
                    Collections.reverse(fillModes);
                    digitMask.reverse();
                }
                for (int i = 0; i < groups.size(); i++) {
                    Object g = groups.get(i);
                    if (!(g instanceof Anchorable) || ((Anchorable) g).isAnchored()) {
                        offerSign = i;
                    }
                    if (g instanceof Pattern && ((Pattern) g).isSign()) {
                        sign = true;
                        if (g == Pattern.BR || g == Pattern.B) {
                            bracket = (Pattern) g;
                        }
                    }
                }
                offerSign++;
                digits = digitMask.length();
                minDigits = digitMask.lastIndexOf("0") + 1;
            }

            /** Ensure matching bracket. Add anchored minus if there is no sign provision. */
            void ensureSignProvision(Mask pair) {
                Pattern inferred = bracket == null && pair.bracket != null ? pair.bracket
                        : pre && !sign && !pair.sign && form != Form.Roman ? Pattern.M : null;
                if (inferred != null) {
                    groups.add(offerSign, inferred);
                    fillModes.add(offerSign, fillModes.isEmpty() ? fillMode : fillModes.get(offerSign - 1));
                }
            }

            void format(StringBuilder s, boolean negative, String integer, String fraction, int exponent,
                        DecimalFormatSymbols symbols) {
                StringBuilder digits = integer == null ? null : pre ? new StringBuilder(integer).reverse()
                        : new StringBuilder(fraction);
                if (digits != null) {
                    // Append zeros to meet the minimum number of digits
                    while (digits.length() < minDigits) {
                        digits.append('0');
                    }
                    // Trim unnecessary zeros
                    int length = digits.length();
                    while (length > minDigits && digits.charAt(length - 1) == '0') {
                        length--;
                    }
                    digits.setLength(length);
                }
                List<Object> parts = new ArrayList<>();
                // d: Accumulate the count of processed digits in the mask
                // f: Accumulate the filler space until a fixed element or the end of the mask is encountered
                for (int i = 0, d = 0, f = 0; i < groups.size();) {
                    Object g = groups.get(i);
                    boolean fillMode = fillModes.get(i);
                    if (g instanceof Literal) {
                        parts.add(((Literal) g).contents);
                    } else if (g instanceof Pattern) {
                        Pattern p = (Pattern) g;
                        if (p.isSign()) {
                            char c = p.getSign(pre, negative);
                            if (c != ' ') {
                                parts.add(c);
                            } else if (fillMode) {
                                f++;
                            }
                        } else if (p == Pattern.CR || p == Pattern.C) {
                            parts.add(symbols.getCurrencySymbol());
                        } else if (p == Pattern.TH || p == Pattern.th) {
                            if (integer != null && !integer.isEmpty()) {
                                String th = getOrdinal(integer);
                                parts.add(p == Pattern.TH ? th.toUpperCase() : th);
                            } else if (fillMode) {
                                f += 2;
                            }
                        } else if (p == Pattern.EEEE || p == Pattern.eeee) {
                            if (digits == null || exponent != 0) {
                                StringBuilder r = new StringBuilder();
                                String e = symbols.getExponentSeparator();
                                r.append(p == Pattern.EEEE ? e : e.toLowerCase());
                                r.append(exponent < 0 ? '-' : '+');
                                if (digits == null) {
                                    r.append("##");
                                } else {
                                    exponent = Math.abs(exponent);
                                    (exponent <= 9 ? r.append('0') : r).append(exponent);
                                }
                                parts.add(r);
                            } else if (fillMode) {
                                f += symbols.getExponentSeparator().length() + 3;
                            }
                        } else if (p == Pattern.RN || p == Pattern.rn) {
                            int n = integer == null || integer.length() > 4 ? Integer.MAX_VALUE
                                    : integer.isEmpty() ? 0 : Integer.parseInt(integer);
                            if (form == Form.Roman || n != 0) {
                                String r = toRoman(form == Form.Roman && (n == 0 || negative)
                                        ? Integer.MAX_VALUE : n);
                                parts.add(p == Pattern.rn ? r.toLowerCase() : r);
                                if (fillMode) {
                                    f += 15 - r.length();
                                }
                            } else if (fillMode) {
                                f += 15;
                            }
                        }
                    } else if (g instanceof Character) {
                        if (digits == null || d < digits.length()) {
                            parts.add(g.equals('G') ? symbols.getGroupingSeparator() : g.equals('D') ? (currency
                                    ? symbols.getMonetaryDecimalSeparator() : symbols.getDecimalSeparator()) : g);
                        } else if (fillMode) {
                            f++;
                        }
                    } else if (g instanceof Integer) {
                        if (digits == null) {
                            StringBuilder r = new StringBuilder();
                            for (int j = 0; j < (int) g; j++) {
                                r.append('#');
                            }
                            parts.add(r);
                        } else if (d < digits.length()) {
                            int e = Math.min(d + (int) g, digits.length());
                            StringBuilder r = new StringBuilder().append(digits, d, e);
                            parts.add(pre ? r.reverse() : r);
                            if (fillMode) {
                                f += d + (int) g - e;
                            }
                        } else if (fillMode) {
                            f += (int) g;
                        }
                        d += (int) g;
                    }

                    i++;
                    if (f > 0 && (i == groups.size() || (groups.get(i) instanceof Anchorable
                            && !((Anchorable) groups.get(i)).isAnchored()))) {
                        StringBuilder r = new StringBuilder();
                        for (; f > 0; f--) {
                            r.append(' ');
                        }
                        parts.add(r);
                    }
                }
                if (pre) {
                    Collections.reverse(parts);
                }
                parts.forEach(s::append);
            }

            @Override
            public String toString() {
                List<Object> mask = new ArrayList<>(groups);
                if (pre) {
                    Collections.reverse(mask);
                }
                return mask.toString();
            }
        }

        class NumericGroupProcessor implements GroupProcessor {
            Mask mask = integerMask;
            boolean currency;
            boolean exponential;
            boolean roman;
            int shift;

            @Override
            public void acceptLiteral(String literal) {
                mask.add(new Literal(literal));
            }

            @Override
            public void acceptGroup(String group, Matcher m) {
                String g = group.toUpperCase();
                if (g.equals("FM")) {
                    mask.toggleFillMode();
                } else if (g.startsWith("V")) {
                    shift += group.length() - 1;
                } else if (g.startsWith("F") || g.startsWith("\"")) {
                    mask.add(new Literal(group));
                } else if (g.startsWith("9") || g.startsWith("0")) {
                    mask.addDigits(group);
                } else if (g.length() == 1 && ",G.D".contains(g)) {
                    if (".D".contains(g) && mask == integerMask) {
                        fractionMask.fillMode = integerMask.fillMode;
                        mask = fractionMask;
                    }
                    mask.add(g.charAt(0));
                } else {
                    Pattern pattern = valueOf(Pattern.class, group);
                    mask.add(pattern);
                    if (pattern == Pattern.CR || pattern == Pattern.C) {
                        currency = true;
                    } else if (pattern == Pattern.EEEE || pattern == Pattern.eeee) {
                        exponential = true;
                    } else if (pattern == Pattern.RN || pattern == Pattern.rn) {
                        roman = true;
                    }
                }
            }
        }

        NumberFormat(String format) {
            NumericGroupProcessor p = new NumericGroupProcessor();
            parse(NUMERIC_TEMPLATE, p, format);

            integerMask.prepare();
            fractionMask.prepare();
            if (integerMask.minDigits == 0 && fractionMask.minDigits == 0) {
                (integerMask.digits > 0 ? integerMask : fractionMask).minDigits = 1;
            }

            form = p.roman && integerMask.digits == 0 && fractionMask.digits == 0 ? Form.Roman
                    : p.exponential ? Form.Exponential : Form.Normal;
            currency = p.currency;
            shift = p.shift;

            integerMask.ensureSignProvision(fractionMask);
            fractionMask.ensureSignProvision(integerMask);
        }

        /**
         * {@link Number#toString()} is the easiest way of obtaining the decimal representation of
         * a {@link Number}, which is in the format {@code /(-)?(\d+)(\.\d+)?(E[-+]\d+)?/}, where
         * the groups denote the sign, integer part, fraction part and exponent respectively. The
         * number formatting algorithm is the following. <ol>
         * <li> Store all significant digits into {@code digits} for ease of computation and
         *      decrement the exponent accordingly. In this normalized form, there is an imaginary
         *      floating-point (decimal separator [.]) at the index {@code digits.length() +
         *      exponent}.
         * <li> Increment the exponent by the shift amount of this number format. At this point,
         *      ‑exponent is the length of the fraction part if exponent ≤ 0.
         * <li> Determine the lengths of the integer and fraction parts. In the exponential form,
         *      they are the number of integer and fraction digits in the pattern, and the
         *      exponent is updated accordingly. Otherwise, they are {@code digits.length() +
         *      exponent} and {@code -exponent} as stated.
         * <li> If the number is subject to rounding, make the rounding according to {@link
         *      RoundingMode#HALF_UP}. This may increase the length of {@code digits} by 1, which
         *      will right-shift the number by 1. However, it will not require additional rounding
         *      since the to-be-rounded digit is guaranteed to be zero in that case.
         * <li> Make paddings as needed. If the length of the integer part <ol type="a">
         *      <li> is greater than the number of integer digits in the pattern, the pattern is
         *           filled with hashes (#).
         *      <li> is greater than the length of {@code digits}, the floating-point (.)
         *           underflows and the integer part is appended with zeros.
         *      <li> is less than zero, the floating-point (.) overflows and the fraction part is
         *           prepended with zeros.
         *      <li> is in the range [0, {@code digits.length()}], the integer and fraction parts
         *           are split normally by the floating-point (.). </ol>
         *      In all cases, neither the integer nor fraction parts contain excess digits. This is
         *      actually an optimization against very low negative exponents.
         * <li> Format the integer and fraction masks. </ol>
         * Useful notes: <ol>
         * <li> Floating-point numbers are always stored and represented in the exponential form.
         *      When the exponent is omitted, the floating-point of the number is aligned to the
         *      decimal separator in the pattern, so there may be overflow/underflow. When the
         *      exponent is printed, the number is left-aligned to the pattern, and the exponent is
         *      updated accordingly. </ol>
         */
        @Override
        public String format(Object input, Locale locale) {
            if (!(input instanceof Number)) {
                throw QueryException.dataException("Input parameter is expected to be numeric");
            }
            DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(locale);
            StringBuilder s = new StringBuilder();
            String value = input.toString();
            boolean negative = value.startsWith("-");

            if (value.equals("NaN") || value.endsWith("Infinity")) {
                // Value is not formattable; pattern is filled with #'s.
                integerMask.format(s, negative, null, null, 0, symbols);
                fractionMask.format(s, negative, null, null, 0, symbols);
            } else {
                int dot = value.indexOf('.');
                int exp = value.indexOf('E', dot + 2);
                int e = exp != -1 ? exp : value.length();
                String integer = value.substring(negative ? 1 : 0, dot != -1 ? dot : e);
                String fraction = dot == -1 ? "" : value.substring(dot + 1, e);
                int exponent = exp == -1 ? 0 : Integer.parseInt(value.substring(exp + 1));
                // Step 1 - Normalized form
                String digits = integer + fraction;
                while (digits.startsWith("0")) {
                    digits = digits.substring(1);
                }
                if (!digits.isEmpty()) {
                    exponent -= fraction.length();
                }
                // Step 2 - Find the actual number
                exponent += shift;
                // Step 3 - Determine the lengths of the integer and fraction
                boolean exponential = form == Form.Exponential;
                int integerLength = exponential ? integerMask.digits : digits.length() + exponent;
                int fractionLength = exponential ? fractionMask.digits : -exponent;
                exponent += digits.length() - integerLength;

                // Step 4 - Rounding
                // integerLength + fractionMask.digits < 0 -> floating-point overflows
                if ((exponential ? digits.length() > integerLength + fractionMask.digits
                            : (fractionLength > fractionMask.digits && integerLength + fractionMask.digits >= 0))
                        && digits.charAt(integerLength + fractionMask.digits) >= '5') {
                    StringBuilder r = new StringBuilder(digits);
                    for (int i = integerLength + fractionMask.digits - 1; i >= 0; i--) {
                        if (r.charAt(i) == '9') {
                            r.setCharAt(i, '0');
                        } else {
                            r.setCharAt(i, (char) (r.charAt(i) + 1));
                            break;
                        }
                    }
                    if (r.charAt(0) == '0') {
                        r.insert(0, '1');
                        if (!exponential) {
                            integerLength++;
                        }
                    }
                    digits = r.toString();
                }

                // Step 5 - Padding
                if (form != Form.Roman && integerLength > integerMask.digits) {
                    // Integer part overflows; pattern is filled with #'s.
                    integerMask.format(s, negative, null, null, 0, symbols);
                    fractionMask.format(s, negative, null, null, 0, symbols);
                } else {
                    if (integerLength > digits.length()) {
                        // Floating-point underflows; integer part is padded wth 0's.
                        StringBuilder r = new StringBuilder(integerLength);
                        r.append(digits);
                        for (int i = digits.length(); i < integerLength; i++) {
                            r.append('0');
                        }
                        integer = r.toString();
                        fraction = "";
                    } else if (integerLength < 0) {
                        // Floating-point overflows; fraction part is padded with 0's.
                        StringBuilder r = new StringBuilder();
                        for (int i = 0; i < Math.min(-integerLength, fractionMask.digits); i++) {
                            r.append('0');
                        }
                        if (-integerLength < fractionMask.digits) {
                            r.append(digits, 0, integerLength + fractionMask.digits);
                        }
                        fraction = r.toString();
                        integer = "";
//                        integerLength = 0;
                    } else {
                        // 0 <= integerLength <= digits.length()
                        integer = digits.substring(0, integerLength);
                        fraction = digits.substring(integerLength,
                                Math.min(integerLength + fractionMask.digits, digits.length()));
                    }

                    // Step 6 - Format the integer and fraction masks
                    if (integer.isEmpty() && integerMask.minDigits > 0) {
                        integer = "0";
                    }
                    integerMask.format(s, negative, integer, fraction, exponent, symbols);
                    fractionMask.format(s, negative, integer, fraction, exponent, symbols);
                }
            }
            return s.toString();
        }

        @Override
        @SuppressWarnings("UnnecessaryToStringCall")
        public String toString() {
            return integerMask.toString() + fractionMask.toString();
        }
    }
}
