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
 * <ol><li> {@code V} pattern can be combined with a decimal point.
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
@SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:DeclarationOrder",
        "checkstyle:ExecutableStatementCount", "checkstyle:InnerAssignment", "checkstyle:MagicNumber",
        "checkstyle:MethodLength", "checkstyle:NestedIfDepth", "checkstyle:NPathComplexity"})
public class Formatter {
//    private static final Pattern DATETIME_TEMPLATE = Pattern.compile(
//            "(FM|TM)?(HH(?:12|24)?|MI|[SMU]S|FF[1-6]|SSSSS?|[AP](?:M|\\.M\\.)|[ap](?:m|\\.m\\.)|"
//            + "Y,YYY|[YI]Y{0,3}|BC|B\\.C\\.|bc|b\\.c\\.|AD|A\\.D\\.|ad|a\\.d\\.|"
//            + "MON(?:TH)?|[Mm]on(?:th)?|MM|DA?Y|[Dd]a?y|I?D{1,3}|DD|W|[WI]W|CC|J|Q|"
//            + "RM|rm|TZ|tz|TZ[HM]?|OF)(TH|th)?");
    private static final Pattern NUMERIC_TEMPLATE = Pattern.compile(
            "FM|[90]+|[,G.D]|BR?|SG?|MI?|PL?|CR?|F?\"[^\"]*\"|V9+|TH|th|EEEE|RN");
    private final BiFunction<Object, DecimalFormatSymbols, String> format;

    public Formatter(String format) {
//        Matcher m = DATETIME_TEMPLATE.matcher(format);
//        FormatString f = new DateFormat(format, m);
//        if (f == null) {
//            m.reset().usePattern(NUMERIC_TEMPLATE);
//            f = new NumberFormat(format, m);
            Matcher m = NUMERIC_TEMPLATE.matcher(format);
            this.format = new NumberFormat(format, m);
//        }
//        this.format = f;
    }

    public String format(Object input, String locale) {
//        if (format instanceof DateFormat && !(input instanceof Temporal)) {
//            throw QueryException.dataException("Input parameter is expected to be date/time");
//        }
        if (format instanceof NumberFormat && !(input instanceof Number)) {
            throw QueryException.dataException("Input parameter is expected to be numeric");
        }
        return format.apply(input, DecimalFormatSymbols.getInstance(Locale.forLanguageTag(locale)));
    }

//    private static class DateFormat implements BiFunction<Object, DecimalFormatSymbols, String> { }

    private static final int[] ARABIC = {1000,  900, 500,  400, 100,   90,  50,   40,  10,    9,   5,    4,   1};
    private static final String[] ROMAN = {"M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"};
    private static final String[] ORDINAL = {"th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th"};

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
    @SuppressWarnings("checkstyle:MultipleVariableDeclarations")
    private static class NumberFormat implements BiFunction<Object, DecimalFormatSymbols, String> {
        private final Form form;
        private final Mask integerMask = new Mask(true), fractionMask = new Mask(false);
        private final boolean currency;
        private final int exponent;

        enum Form { Normal, Exponential, Roman }

        interface Anchorable {
            boolean isAnchored();
        }

        enum Pattern implements Anchorable {
            BR, B, SG, S, MI, M, PL, P, CR, C, TH, th, EEEE, RN;

            static final List<Pattern> SIGN = Arrays.asList(BR, B, SG, S, MI, M, PL, P);
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

            static final List<Pattern> ANCHORED = Arrays.asList(B, S, M, P, C, TH, th, EEEE, RN);
            @Override
            public boolean isAnchored() {
                return ANCHORED.contains(this);
            }
        }

        static class Literal implements Anchorable {
            final String contents;
            final boolean anchored;

            /** In the forms: {@code .+}, {@code F.*}, {@code "[^"]*"}, {@code F"[^"]*"}. */
            Literal(String literal) {
                anchored = !literal.startsWith("F");
                int skip = anchored ? 0 : 1;
                int quote = literal.charAt(skip) == '"' && literal.length() >= skip + 2
                        && literal.endsWith("\"") ? 1 : 0;
                contents = literal.substring(skip + quote, literal.length() - quote);
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
        class Mask extends ArrayList<Object> {
            final boolean pre;
            final StringBuilder digitMask = new StringBuilder();
            final List<Boolean> fillModes = new ArrayList<>();
            boolean fillMode = true;
            int offerSign = -1, digits, minDigits;
            boolean sign = false;
            Pattern bracket = null;

            Mask(boolean pre) {
                this.pre = pre;
            }

            void toggleFillMode() {
                fillMode = !fillMode;
            }

            @Override
            public boolean add(Object g) {
                fillModes.add(fillMode);
                return super.add(g);
            }

            void addDigits(String digits) {
                digitMask.append(digits);
                add(digits.length());
            }

            void prepare() {
                if (pre) {
                    Collections.reverse(this);
                    Collections.reverse(fillModes);
                    digitMask.reverse();
                }
                for (int i = 0; i < size(); i++) {
                    Object g = get(i);
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
                    add(offerSign, inferred);
                    fillModes.add(offerSign, fillModes.get(Math.max(offerSign - 1, 0)));
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
                for (int i = 0, d = 0, f = 0; i < size();) {
                    Object g = get(i);
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
                            if (integer != null) {
                                String th = integer.endsWith("11") || integer.endsWith("12") || integer.endsWith("13")
                                        ? "th" : ORDINAL[integer.charAt(integer.length() - 1) - 48];
                                parts.add(p == Pattern.TH ? th.toUpperCase() : th);
                            } else if (fillMode) {
                                f += 2;
                            }
                        } else if (p == Pattern.EEEE) {
                            if (digits == null || exponent != 0) {
                                StringBuilder r = new StringBuilder();
                                r.append(symbols.getExponentSeparator());
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
                        } else if (p == Pattern.RN) {
                            if (integer != null) {
                                long n = integer.isEmpty() ? 0 : Long.parseLong(integer);
                                StringBuilder r = new StringBuilder(15);
                                if (((n == 0 || negative) && form == Form.Roman) || n > 3999) {
                                    for (int j = 0; j < 15; j++) {
                                        r.append('#');
                                    }
                                } else {
                                    for (int j = 0; j < ARABIC.length; j++) {
                                        for (; n >= ARABIC[j]; n -= ARABIC[j]) {
                                            r.append(ROMAN[j]);
                                        }
                                    }
                                }
                                parts.add(r);
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
                    if (f > 0 && (i == size() || (get(i) instanceof Anchorable && !((Anchorable) get(i)).isAnchored()))) {
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
                List<Object> mask = new ArrayList<>(this);
                if (pre) {
                    Collections.reverse(mask);
                }
                return mask.toString();
            }
        }

        NumberFormat(String format, Matcher m) {
            Mask mask = integerMask;
            boolean currency = false, exponential = false, roman = false;
            int i = 0, exponent = 0;

            for (; m.find(); i = m.end()) {
                if (m.start() > i) {
                    mask.add(new Literal(format.substring(i, m.start())));
                }
                String group = m.group();
                if (group.equals("FM")) {
                    mask.toggleFillMode();
                } else if (group.startsWith("V")) {
                    exponent += group.length() - 1;
                } else if (group.startsWith("F") || group.startsWith("\"")) {
                    mask.add(new Literal(group));
                } else if (group.startsWith("9") || group.startsWith("0")) {
                    mask.addDigits(group);
                } else if (group.length() == 1 && ",G.D".contains(group)) {
                    if (".D".contains(group) && mask == integerMask) {
                        fractionMask.fillMode = integerMask.fillMode;
                        mask = fractionMask;
                    }
                    mask.add(group.charAt(0));
                } else {
                    Pattern pattern = Pattern.valueOf(group);
                    mask.add(pattern);
                    if (pattern == Pattern.CR || pattern == Pattern.C) {
                        currency = true;
                    } else if (pattern == Pattern.EEEE) {
                        exponential = true;
                    } else if (pattern == Pattern.RN) {
                        roman = true;
                    }
                }
            }
            if (i < m.regionEnd()) {
                mask.add(new Literal(format.substring(i)));
            }

            integerMask.prepare();
            fractionMask.prepare();

            form = roman && integerMask.digits == 0 && fractionMask.digits == 0 ? Form.Roman
                    : exponential ? Form.Exponential : Form.Normal;
            this.currency = currency;
            this.exponent = exponent;

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
         * <li> Increment the exponent by the exponent of this number format. At this point,
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
        public String apply(Object input, DecimalFormatSymbols symbols) {
            StringBuilder s = new StringBuilder();
            String value = input.toString();
            boolean negative = value.startsWith("-");

            if (value.equals("NaN") || value.endsWith("Infinity")) {
                // Value is not formattable; pattern is filled with #'s.
                integerMask.format(s, negative, null, null, 0, symbols);
                fractionMask.format(s, negative, null, null, 0, symbols);
            } else {
                int t = negative ? 1 : 0, begin = value.charAt(t) == '0' ? t + 1 : t;
                int dot = value.indexOf('.'), exp = dot == -1 ? -1 : value.indexOf('E', dot + 2);
                String integer = value.substring(begin, dot != -1 ? dot : value.length());
                String fraction = (dot == -1 ? "" : value.substring(dot + 1, exp != -1 ? exp : value.length()));
                int exponent = exp == -1 ? 0 : Integer.parseInt(value.substring(exp + 1));
                // Step 1 - Normalized form
                String digits = integer + fraction;
                exponent -= fraction.length();
                while (digits.startsWith("0")) {
                    digits = digits.substring(1);
                }
                // Step 2 - Find the actual number
                exponent += this.exponent;
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
                    integerMask.format(s, negative, null, null, exponent, symbols);
                    fractionMask.format(s, negative, null, null, exponent, symbols);
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
