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
import sun.misc.FormattedFloatingDecimal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of <a href="https://www.postgresql.org/docs/14/functions-formatting.html">
 * PostgreSQL <code>to_char()</code></a> with the following limitations
 * <ol><li> {@code FX} prefix is not implemented.
 *     <li> {@code SG} pattern can only be specified as a prefix or a suffix. </ol>
 * and the following relaxations
 * <ol><li> {@code V} pattern can be combined with a decimal point. </ol>
 */
@SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:DeclarationOrder",
        "checkstyle:ExecutableStatementCount", "checkstyle:InnerAssignment", "checkstyle:MagicNumber",
        "checkstyle:MethodLength", "checkstyle:NeedBraces", "checkstyle:NestedIfDepth", "checkstyle:NPathComplexity"})
public class Formatter {
    private static final DecimalFormatSymbols SYMBOLS = DecimalFormatSymbols.getInstance(Locale.getDefault());
    private static final String NUMERIC_PREFIX_SUFFIX = "PR|MI|PL|SG|S|L";
    private static final Pattern DATETIME_TEMPLATE = Pattern.compile(
            "(FM|TM)?(HH(?:12|24)?|MI|[SMU]S|FF[1-6]|SSSSS?|[AP](?:M|\\.M\\.)|[ap](?:m|\\.m\\.)|"
            + "Y,YYY|[YI]Y{0,3}|BC|B\\.C\\.|bc|b\\.c\\.|AD|A\\.D\\.|ad|a\\.d\\.|"
            + "MON(?:TH)?|[Mm]on(?:th)?|MM|DA?Y|[Dd]a?y|I?D{1,3}|DD|W|[WI]W|CC|J|Q|"
            + "RM|rm|TZ|tz|TZ[HM]?|OF)(TH|th)?");
    private static final Pattern NUMERIC_TEMPLATE = Pattern.compile(String.format(
            "(FM)?((%s)?(([90][90,G]*)([.D][90]+)?(V9+)?)(%<s|TH|th|EEEE)?|RN)", NUMERIC_PREFIX_SUFFIX));
    private final List<FormatString> parts = new ArrayList<>();
    private final boolean isDate;

    public Formatter(String format) {
//        Matcher m = DATETIME_TEMPLATE.matcher(format);
//        parse(m, format, Integer.MAX_VALUE, DateFormat::new);
//        isDate = !parts.isEmpty();
//        if (!isDate) {
//            m.reset().usePattern(NUMERIC_TEMPLATE);
            isDate = false;
            Matcher m = NUMERIC_TEMPLATE.matcher(format);
            parse(m, format, 1, NumberFormat::new);
//        }
        if (parts.isEmpty())
            parts.add(new FixedString(format));
    }

    private void parse(Matcher m, String format, int count, Function<Matcher, FormatString> constructor) {
        for (int i = 0, j = 0; i < count && m.find(); i++, j = m.end()) {
            if (m.start() > j)
                parts.add(new FixedString(format.substring(j, m.start())));
            parts.add(constructor.apply(m));
        }
        if (!parts.isEmpty() && m.end() < m.regionEnd())
            parts.add(new FixedString(format.substring(m.end())));
    }

    public String format(Object input) {
        if (isDate && !(input instanceof Temporal))
            throw QueryException.dataException("Input parameter is expected to be date/time");
        if (!isDate && !(input instanceof Number))
            throw QueryException.dataException("Input parameter is expected to be numeric");
        return parts.stream().collect(StringBuilder::new, (s, part) -> part.format(s, input),
                StringBuilder::append).toString();
    }

    private interface FormatString<T> {
        default void format(StringBuilder s) { }

        default void format(StringBuilder s, T input) {
            format(s);
        }
    }

    private static class FixedString implements FormatString {
        String contents;

        FixedString(String contents) {
            this.contents = contents;
        }

        @Override
        public void format(StringBuilder s) {
            s.append(contents);
        }
    }

//    private class DateFormat implements FormatString<Temporal> { }

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
     *          {@link FormattedFloatingDecimal} for {@link Float} and {@link Double}, and by using
     *          {@link BigDecimal#BigDecimal(BigInteger, int, MathContext) BigDecimal(BigInteger,
     *          scale, precision)}. Both {@link FormattedFloatingDecimal} and {@link BigDecimal}
     *          are using the decimal representation of floating point numbers, but for the former,
     *          the rounding mode is not configurable. </ol>
     */
    @SuppressWarnings("checkstyle:MultipleVariableDeclarations")
    private static class NumberFormat implements FormatString<Number> {
        private final boolean fillMode;
        private final String prefix, pattern, suffix;
        private final int integerDigits, minIntegerDigits, fractionDigits, minFractionDigits, exponent;
        /**
         * Digit mask with grouping/decimal separator(s). {@code '0'} always prints the digit even
         * if is a leading/trailing zero. {@code ' '} does not print the digit in that case.
         */
        private final char[] integerMask, fractionMask;
        /**
         * Anchored sign. {@code '\0'} prints nothing. {@code '-'} prints a minus sign if the
         * number is negative. {@code '+'} prints a minus sign if the number is negative;
         * otherwise, prints a plus sign.
         */
        private final char preSign, postSign;

        NumberFormat(Matcher m) {
            fillMode = !"FM".equals(m.group(1));
            if (m.group(4) != null) {
                prefix = m.group(3);
                pattern = m.group(4);
                suffix = m.group(8);

                if ("S".equals(prefix))
                    preSign = '+';
                else if (!(hasSign(prefix) || hasSign(suffix)))
                    preSign = '-';
                else preSign = 0;

                postSign = "S".equals(suffix) ? '+' : 0;

                int digits = 0, zero = -1;
                integerMask = new char[m.group(5).length()];
                for (int i = 0; i < integerMask.length; i++) {
                    char c = m.group(5).charAt(i);
                    if (c == '9' || c == '0') {
                        if (c == '0' && zero == -1)
                            zero = digits;
                        digits++;
                        integerMask[i] = zero == -1 ? ' ' : '0';
                    } else {
                        integerMask[i] = c == '.' ? c : SYMBOLS.getGroupingSeparator();
                    }
                }
                integerDigits = digits;
                minIntegerDigits = zero == -1 ? 0 : digits - zero;

                if (m.group(6) != null && !"TH".equalsIgnoreCase(suffix)) {
                    fractionMask = new char[m.group(6).length()];
                    fractionMask[0] = m.group(6).startsWith(".") ? '.'
                            : ("L".equals(prefix) || "L".equals(suffix)
                                    ? SYMBOLS.getMonetaryDecimalSeparator() : SYMBOLS.getDecimalSeparator());
                    zero = -1;
                    for (int i = fractionMask.length - 1; i > 0; i--) {
                        char c = m.group(6).charAt(i);
                        if (c == '0' && zero == -1)
                            zero = i;
                        fractionMask[i] = zero == -1 ? ' ' : '0';
                    }
                    fractionDigits = fractionMask.length - 1;
                    minFractionDigits = zero == -1 ? 0 : zero;
                } else {
                    fractionMask = null;
                    fractionDigits = minFractionDigits = 0;
                }

                exponent = m.group(7) == null ? 0 : m.group(7).length() - 1;
            } else {
                prefix = suffix = null;
                pattern = m.group(2);
                integerDigits = minIntegerDigits = fractionDigits = minFractionDigits = exponent = 0;
                integerMask = fractionMask = null;
                preSign = postSign = 0;
            }
        }

        @Override
        public void format(StringBuilder s, Number input) {
            if (pattern.equals("RN")) {
                // Roman Numerals
                long n = input.longValue();
                if (n < 1 || n > 3999)
                    throw QueryException.dataException(
                            "Only values between 1 and 3999 can be converted to roman numerals: " + n);
                StringBuilder r = new StringBuilder(15);
                for (int i = 0; i < ARABIC.length; i++)
                    for (; n >= ARABIC[i]; n -= ARABIC[i])
                        r.append(ROMAN[i]);
                if (fillMode)
                    for (int i = r.length(); i < 15; i++)
                        s.append(' ');
                s.append(r);
            } else {
                /* Number#toString() is the easiest way of obtaining the decimal representation of
                 * a Number, which is in the format /(-)?(\d+)(\.\d+)?(E[-+]\d+)?/, where the
                 * groups denote the sign, integer part, fraction part and exponent respectively.
                 * The number formatting algorithm is the following.
                 *  1. Merge the integer and fraction parts into `digits` for ease of computation
                 *     and decrement the exponent accordingly. In this normalized form, there is an
                 *     imaginary floating-point (decimal separator [.]) at the index
                 *     `digits.length() + exponent`.
                 *  2. Increment the exponent by the exponent of this number format. At this point,
                 *     -exponent is the length of the fraction part if exponent <= 0.
                 *  3. Determine the lengths of the integer and fraction parts. In the exponential
                 *     form, they are the number of integer and fraction digits in the pattern, and
                 *     the exponent is updated accordingly. Otherwise, they are `digits.length() +
                 *     exponent` and `-exponent` as stated.
                 *  4. If the number is subject to rounding, make the rounding according to
                 *     RoundingMode#HALF_UP. This may increase the length of `digits` by 1, which
                 *     will right-shift the number by 1. However, it will not require additional
                 *     rounding since the rightmost digit is guaranteed to be zero in that case.
                 *  5. Make paddings as needed. If the length of the integer part
                 *      a. is greater than the number of integer digits in the pattern, the pattern
                 *         is filled with hashes (#).
                 *      b. is greater than the length of `digits`, the floating-point (.)
                 *         underflows and the integer part is appended with zeros.
                 *      c. is less than zero, the floating-point (.) overflows and the fraction
                 *         part is prepended with zeros.
                 *      d. is the range [0, digits.length()], the integer and fraction parts are
                 *         split normally by the floating-point (.).
                 *     In all cases, neither the integer nor fraction parts contain excess digits.
                 *     This is actually an optimization against very low negative exponents.
                 *  6. Fill the integer and fraction digit masks by taking anchored signs into
                 *     account. Print a part of or the complete masks depending on the fill mode.
                 *     In the ordinal form, fraction part is not printed. In the exponent form, the
                 *     exponent is printed at the end.
                 * Useful notes:
                 *  1. Floating-point numbers are always stored and represented in the exponential
                 *     form. When the exponent is omitted, the floating-point of the number is
                 *     aligned to the decimal separator in the pattern, so there may be overflow/
                 *     underflow. When the exponent is printed, the number is left-aligned to the
                 *     pattern, and the exponent is updated accordingly.
                 */
                String value = input.toString();
                boolean negative = value.startsWith("-");
                if (preSign == 0)
                    affix(s, true, negative);

                if (value.equals("NaN"))
                    s.append(SYMBOLS.getNaN());
                else if (value.endsWith("Infinity"))
                    s.append(SYMBOLS.getInfinity());
                else {
                    int t = negative ? 1 : 0, begin = value.charAt(t) == '0' ? t + 1 : t;
                    int dot = value.indexOf('.'), exp = dot == -1 ? -1 : value.indexOf('E', dot + 2);
                    String integer = value.substring(begin, dot != -1 ? dot : value.length());
                    String fraction = (dot == -1 ? "" : value.substring(dot + 1, exp != -1 ? exp : value.length()));
                    int exponent = exp == -1 ? 0 : Integer.parseInt(value.substring(exp + 1));
                    // Step 1 - Normalized form
                    String digits = integer + fraction;
                    exponent -= fraction.length();
                    // Step 2 - Find the actual number
                    exponent += this.exponent;
                    // Step 3 - Determine the lengths of the integer and fraction
                    boolean exponential = "EEEE".equals(suffix);
                    int integerLength = exponential ? integerDigits : digits.length() + exponent;
                    int fractionLength = exponential ? fractionDigits : -exponent;
                    exponent += digits.length() - integerLength;

                    // Step 4 - Rounding
                    // integerLength + fractionDigits < 0 -> floating-point overflows
                    if ((exponential ? digits.length() > integerLength + fractionDigits
                                : (fractionLength > fractionDigits && integerLength + fractionDigits >= 0))
                            && digits.charAt(integerLength + fractionDigits) >= '5') {
                        StringBuilder r = new StringBuilder(digits);
                        for (int i = integerLength + fractionDigits - 1; i >= 0; i--)
                            if (r.charAt(i) == '9')
                                r.setCharAt(i, '0');
                            else {
                                r.setCharAt(i, (char) (r.charAt(i) + 1));
                                break;
                            }
                        if (r.charAt(0) == '0') {
                            r.insert(0, '1');
                            if (!exponential)
                                integerLength++;
                        }
                        digits = r.toString();
                    }

                    // Step 5 - Padding
                    if (integerLength > integerDigits) {
                        // Integer part overflows; pattern is filled with #'s.
                        for (char c : integerMask)
                            s.append(c == ' ' || c == '0' ? '#' : c);
                        if (fractionDigits > 0) {
                            s.append(fractionMask[0]);
                            for (int i = 0; i < fractionDigits; i++)
                                s.append('#');
                        }
                    } else {
                        if (integerLength > digits.length()) {
                            // Floating-point underflows; integer part is padded wth 0's.
                            StringBuilder r = new StringBuilder(integerLength);
                            r.append(digits);
                            for (int i = digits.length(); i < integerLength; i++)
                                r.append('0');
                            integer = r.toString();
                            fraction = "";
                        } else if (integerLength < 0) {
                            // Floating-point overflows; fraction part is padded with 0's.
                            StringBuilder r = new StringBuilder();
                            for (int i = 0; i < Math.min(-integerLength, fractionDigits); i++)
                                r.append('0');
                            if (-integerLength < fractionDigits)
                                r.append(digits, 0, integerLength + fractionDigits);
                            fraction = r.toString();
                            integer = "";
                            integerLength = 0;
                        } else {
                            // 0 <= integerLength <= digits.length()
                            integer = digits.substring(0, integerLength);
                            fraction = digits.substring(integerLength,
                                    Math.min(integerLength + fractionDigits, digits.length()));
                        }

                        // Step 6.1 - Fill and print the integer digit mask

                        // Copy the integer mask by leaving room for the anchored sign.
                        int i = preSign == 0 ? 0 : 1;
                        char[] r = new char[i + integerMask.length];
                        System.arraycopy(integerMask, 0, r, i, integerMask.length);
                        // Skip over unoccupied digits by taking grouping separators into account.
                        // Clear grouping separators that does not come after a digit.
                        // Save the index of the first digit to `j`.
                        int j, k = integerDigits - integerLength;
                        for (j = -1; k > 0; i++)
                            if (r[i] == ' ' || r[i] == '0') {
                                if (r[i] == '0' && j == -1)
                                    j = i;
                                k--;
                            } else if (j != -1)
                                r[i] = ' ';
                        if (j == -1)
                            j = i;
                        // Put the anchored sign if there is any.
                        // Update `j` to reflect the starting index of the potentially signed integer.
                        if (preSign != 0) {
                            r[j - 1] = negative ? '-' : preSign == '+' ? '+' : ' ';
                            if (r[j - 1] != ' ')
                                j--;
                        }
                        // Copy digits into the mask by taking grouping separators into account.
                        for (k = 0; i < r.length; i++)
                            if (r[i] == ' ' || r[i] == '0')
                                r[i] = integer.charAt(k++);
                        // Print the mask by starting from `fillMode ? 0 : j`.
                        if (fillMode)
                            j = 0;
                        s.append(r, j, r.length - j);

                        // Step 6.2 - Fill and print the fraction digit mask

                        if (fractionDigits > 0) {
                            // Copy the fraction mask by leaving room for the anchored sign.
                            k = postSign == 0 ? 0 : 1;
                            r = Arrays.copyOf(fractionMask, fractionMask.length + k);
                            // Copy digits into the mask by taking decimal separator into account.
                            // Save the length of the longest prefix without trailing zeros to `j`.
                            for (i = 0, j = 0; i < fraction.length(); i++) {
                                r[i + 1] = fraction.charAt(i);
                                if (r[i + 1] != '0')
                                    j = i + 1;
                            }
                            // Update `j` to reflect the length of the fraction including the decimal separator.
                            j = Math.max(minFractionDigits, j);
                            if (j > 0)
                                j++;
                            // Clear unnecessary trailing zeros and decimal separator.
                            for (i = j; i < r.length; i++)
                                r[i] = ' ';
                            // Put the anchored sign if there is any.
                            if (postSign != 0)
                                r[j] = negative ? '-' : '+';
                            // Print the mask up to `fillMode ? r.length : j + k`.
                            j = fillMode ? r.length : j + k;
                            if (j > 0)
                                s.append(r, 0, j);
                        } else if ("TH".equalsIgnoreCase(suffix) && integerLength > 0) {
                            // For ordinals, the fraction part is suppressed.
                            String th = integer.endsWith("11") || integer.endsWith("12") || integer.endsWith("13")
                                    ? "th" : ORDINAL[integer.charAt(integerLength - 1) - 48];
                            s.append("TH".equals(suffix) ? th.toUpperCase() : th);
                        } else if (postSign != 0) {
                            // Anchored sign suffix is handled here if there is no fraction.
                            s.append(negative ? '-' : '+');
                        }

                        // Step 6.3 - Print the exponent
                        if (exponential && exponent != 0) {
                            s.append(SYMBOLS.getExponentSeparator());
                            s.append(exponent < 0 ? '-' : '+');
                            exponent = Math.abs(exponent);
                            if (exponent <= 9)
                                s.append('0');
                            s.append(exponent);
                        }
                    }
                }

                if (postSign == 0)
                    affix(s, false, negative);
            }
        }

        private boolean hasSign(String affix) {
            return "PR".equals(affix) || "MI".equals(affix) || "PL".equals(affix)
                    || "SG".equals(affix) || "S".equals(affix);
        }

        private void affix(StringBuilder s, boolean pre, boolean negative) {
            String affix = pre ? prefix : suffix;
            char c = 0;
            if ("PR".equals(prefix) || "PR".equals(suffix))
                c = negative ? (pre ? '<' : '>') : ' ';
            else if ("MI".equals(affix))
                c = negative ? '-' : ' ';
            else if ("PL".equals(affix))
                c = negative ? ' ' : '+';
            else if ("SG".equals(affix))
                c = negative ? '-' : '+';
            if (c != 0) {
                if (c != ' ' || fillMode)
                    s.append(c);
            } else if ("L".equals(affix))
                s.append(SYMBOLS.getCurrencySymbol());
        }

        @Override
        public String toString() {
            StringBuilder s = new StringBuilder();
            if (preSign != 0) s.append(preSign);
            s.append(integerMask).append(fractionMask);
            if (postSign != 0) s.append(postSign);
            return s.toString();
        }
    }
}
