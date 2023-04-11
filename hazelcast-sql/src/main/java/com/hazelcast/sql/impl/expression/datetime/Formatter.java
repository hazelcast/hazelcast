/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.sql.impl.QueryException;

import javax.annotation.Nonnull;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.temporal.WeekFields.ISO;

/**
 * <h2> Date/Time Formatting
 * <table>
 * <tr><th> Pattern                     <th> Description
 * <tr><td> {@code HH}, {@code HH12}    <td> hour of day (1–12)
 * <tr><td> {@code HH24}                <td> hour of day (0–23)
 * <tr><td> {@code MI}                  <td> minute of hour (0–59)
 * <tr><td> {@code SS}                  <td> second of minute (0–59)
 * <tr><td> {@code MS}, {@code FF3}     <td> millisecond (0–999)
 * <tr><td> {@code US}, {@code FF6}     <td> microsecond (0–999999)
 * <tr><td> {@code FF1}                 <td> tenth of second (0–9)
 * <tr><td> {@code FF2}                 <td> hundredth of second (0–99)
 * <tr><td> {@code FF4}                 <td> tenth of a millisecond (0–9999)
 * <tr><td> {@code FF5}                 <td> hundredth of a millisecond (0–99999)
 * <tr><td> {@code SSSS}, {@code SSSSS} <td> seconds past midnight (0–86399)
 * <tr><td> {@code AM}, {@code am}, {@code PM}, {@code pm}
 *                                      <td> meridiem indicator (without periods)
 * <tr><td> {@code A.M.}, {@code a.m.}, {@code P.M.}, {@code p.m.}
 *                                      <td> meridiem indicator (with periods)
 * <tr><td> {@code Y,YYY}               <td> year of era (4 or more digits) with comma
 * <tr><td> {@code YYYY}                <td> year of era (4 or more digits)
 * <tr><td> {@code YYY}                 <td> last 3 digits of year of era
 * <tr><td> {@code YY}                  <td> last 2 digits of year of era
 * <tr><td> {@code Y}                   <td> last digit of year of era
 * <tr><td> {@code IYYY}                <td> ISO 8601 week-numbering year (4 or more digits)
 * <tr><td> {@code IYY}                 <td> last 3 digits of ISO 8601 week-numbering year
 * <tr><td> {@code IY}                  <td> last 2 digits of ISO 8601 week-numbering year
 * <tr><td> {@code I}                   <td> last digit of ISO 8601 week-numbering year
 * <tr><td> {@code BC}, {@code bc}, {@code AD}, {@code ad}
 *                                      <td> era indicator (without periods)
 * <tr><td> {@code B.C.}, {@code b.c.}, {@code A.D.}, {@code a.d.}
 *                                      <td> era indicator (with periods)
 * <tr><td> {@code MONTH}               <td> full uppercase month name (space-padded to 9 chars)
 * <tr><td> {@code Month}               <td> full capitalized month name (space-padded to 9 chars)
 * <tr><td> {@code month}               <td> full lowercase month name (space-padded to 9 chars)
 * <tr><td> {@code MON}                 <td> abbreviated uppercase month name (3 chars in English,
 *                                           localized lengths vary)
 * <tr><td> {@code Mon}                 <td> abbreviated capitalized month name (3 chars in
 *                                           English, localized lengths vary)
 * <tr><td> {@code mon}                 <td> abbreviated lowercase month name (3 chars in English,
 *                                           localized lengths vary)
 * <tr><td> {@code MM}                  <td> month number (1–12)
 * <tr><td> {@code DAY}                 <td> full uppercase day name (space-padded to 9 chars)
 * <tr><td> {@code Day}                 <td> full capitalized day name (space-padded to 9 chars)
 * <tr><td> {@code day}                 <td> full lowercase day name (space-padded to 9 chars)
 * <tr><td> {@code DY}                  <td> abbreviated uppercase day name (3 chars in English,
 *                                           localized lengths vary)
 * <tr><td> {@code Dy}                  <td> abbreviated capitalized day name (3 chars in English,
 *                                           localized lengths vary)
 * <tr><td> {@code dy}                  <td> abbreviated lowercase day name (3 chars in English,
 *                                           localized lengths vary)
 * <tr><td> {@code DDD}                 <td> day of year (1–366)
 * <tr><td> {@code IDDD}                <td> day of ISO 8601 week-numbering year (1–371; day 1 of
 *                                           the year is Monday of the first ISO week)
 * <tr><td> {@code DD}                  <td> day of month (1–31)
 * <tr><td> {@code D}                   <td> day of the week, Monday (1) to Sunday (7)
 * <tr><td> {@code ID}                  <td> ISO 8601 day of the week, Monday (1) to Sunday (7)
 * <tr><td> {@code W}                   <td> week of month (1–5) (the first week starts on the
 *                                           first day of the month)
 * <tr><td> {@code WW}                  <td> week number of year (1–53) (the first week starts on
 *                                           the first day of the year)
 * <tr><td> {@code IW}                  <td> week number of ISO 8601 week-numbering year (1–53; the
 *                                           first Thursday of the year is in week 1)
 * <tr><td> {@code CC}                  <td> century of era (2 digits) (the twenty-first century
 *                                           starts on 2001-01-01)
 * <tr><td> {@code J}                   <td> Julian Date (integer days since November 24, 4714 BC
 *                                           at local midnight)
 * <tr><td> {@code Q}                   <td> quarter of year (1-4)
 * <tr><td> {@code RY}                  <td> year of era in uppercase Roman numerals
 * <tr><td> {@code ry}                  <td> year of era in lowercase Roman numerals
 * <tr><td> {@code RM}                  <td> month number in uppercase Roman numerals (I–XII)
 * <tr><td> {@code rm}                  <td> month number in lowercase Roman numerals (i–xii)
 * <tr><td> {@code RD}                  <td> day of month in uppercase Roman numerals (I–XXXI)
 * <tr><td> {@code rd}                  <td> day of month in lowercase Roman numerals (i–xxxi)
 * <tr><td> {@code TZ}                  <td> uppercase time-zone abbreviation (e.g. GMT, UTC)
 * <tr><td> {@code tz}                  <td> lowercase time-zone abbreviation (e.g. gmt, utc)
 * <tr><td> {@code TZH}                 <td> time-zone hours (e.g. +3)
 * <tr><td> {@code TZM}                 <td> time-zone minutes (0-59)
 * <tr><td> {@code OF}                  <td> time-zone offset from UTC (e.g. +03:00)
 * </table>
 *
 * <table>
 * <tr><th> Modifier          <th> Description
 * <tr><td> {@code FM} prefix <td> enable fill mode (suppress padding)
 * <tr><td> {@code TH} suffix <td> uppercase ordinal number suffix (English only)
 * <tr><td> {@code th} suffix <td> lowercase ordinal number suffix (English only)
 * </table>
 *
 * <br><h2> Numeric Formatting
 * <table>
 * <tr><th> Pattern            <th> Description
 * <tr><td> {@code 9}          <td> digit position (can be dropped if insignificant)
 * <tr><td> {@code 0}          <td> digit position (will not be dropped, even if insignificant)
 * <tr><td> {@code .} (period) <td> decimal separator
 * <tr><td> {@code D}          <td> localized decimal separator
 * <tr><td> {@code ,} (comma)  <td> grouping separator
 * <tr><td> {@code G}          <td> localized grouping separator
 * <tr><td> {@code V}          <td> shift specified number of digits (e.g. V99 = x10<sup>2</sup>)
 * <tr><td> {@code TH}         <td> uppercase ordinal suffix for the integer part (English only)
 * <tr><td> {@code th}         <td> lowercase ordinal suffix for the integer part (English only)
 * <tr><td> {@code EEEE}       <td> exponent for scientific notation (e.g. E+03, x10^+03)
 * <tr><td> {@code eeee}       <td> lowercase exponent for scientific notation (e.g. e+03, x10^+03)
 * <tr><td> {@code RN}         <td> uppercase Roman numeral for the integer part
 * <tr><td> {@code rn}         <td> lowercase Roman numeral for the integer part
 * <tr><td> {@code FM}         <td> enable fill mode (suppress padding)
 * </table>
 *
 * <table>
 * <tr><th> Fixed      <th> Anchored  <th> Description
 * <tr><td> {@code BR} <td> {@code B} <td> negative value in angle brackets
 * <tr><td> {@code SG} <td> {@code S} <td> sign
 * <tr><td> {@code MI} <td> {@code M} <td> minus sign if number is negative
 * <tr><td> {@code PL} <td> {@code P} <td> plus sign if number is non-negative
 * <tr><td> {@code CR} <td> {@code C} <td> currency symbol or ISO 4217 currency code
 * </table>
 *
 * <br><b> Notes </b><ul>
 * <li> The format string consists of <em>the integer and fraction parts</em>, which are split at
 *      the first decimal separator, or just after the last digit position, or the end of the
 *      format string depending on availability. The order of processing is right-to-left in the
 *      integer part and left-to-right in the fraction part.
 * <li> If the format string contains {@code EEEE} or {@code eeee} patterns, it is said to be in
 *      <em>the exponential form</em>, in which no overflow is possible unless the number is
 *      infinite. If it contains {@code RN} or {@code rn} patterns and no digit positions, it is in
 *      <em>the Roman form</em>, in which there is an overflow unless number ∈ [1, 4000).
 *      Otherwise, the format string is in <em>the normal form</em>, in which the number overflows
 *      only if it requires more digit positions than specified for the integer part. In this form,
 *      {@code RN} and {@code rn} patterns format the integer part if |number| < 4000; otherwise,
 *      they switch to the overflow mode.
 * <li> In an overflow; digit positions print a single hash (#), {@code EEEE} and {@code eeee}
 *      patterns print +## as the exponent, {@code RN} and {@code rn} patterns print 15 hashes, and
 *      {@code TH} and {@code th} patterns print 2 spaces if the number is infinite. The other
 *      patterns print what they print when there is no overflow. Note that NaN (non-a-number) is
 *      considered positive.
 * <li> In the normal and exponential forms, if there is no negative sign provision and there is at
 *      least one digit position, an {@code M} pattern is prepended to the integer part. Similarly,
 *      if only one part has {@code BR} and/or {@code B} patterns, the latest bracket in the order
 *      of processing is inserted to the opposite part. The inferred sign is inserted so that it
 *      encloses all non-fixed patterns in the part to which it is inserted. </ul>
 *
 * <br><h2> General Notes <ol>
 * <li> <b>Lowercase variants</b> of patterns are also accepted. If there is no special meaning of
 *      the lowercase variant, it has the same effect as its uppercase version.
 * <li> {@code FM} pattern enables <b><em>the fill mode</em></b>, which suppresses padding.
 *      In date formats: <ol type="a">
 *      <li> If padding is enabled, numeric fields are left-padded with zeros and textual fields are
 *           left-padded with spaces.
 *      <li> The padding space is printed immediately, i.e. it is not possible to float the fields
 *           to one side.
 *      </ol>
 *      In numeric formats: <ol type="a">
 *      <li> If padding is enabled; {@code 9} pattern prints a single space if it corresponds to a
 *           leading/trailing zero, decimal/grouping separators print a single space if they are
 *           not in between digits, {@code TH} pattern prints 2 spaces if the number is infinite,
 *           {@code RN} pattern pads the Roman numeral to meet 15 characters, {@code BR} pattern
 *           prints 2 spaces if the number is non-negative, and {@code MI}/{@code PL} patterns
 *           print a single space if the number is non-negative/negative respectively.
 *      <li> The padding space is not printed until a <b><em>fixed</em></b> pattern or the end of
 *           the format string is encountered. As a result, unfixed, or <b><em>anchored</em></b>,
 *           patterns float right within the extra space in the integer part and float left in the
 *           fraction part. Digit positions and decimal/grouping separators cannot float for
 *           obvious reasons, but they are considered "transparent" while anchoring other patterns.
 *      <li> Zero-padding and space-padding are completely orthogonal, which makes it possible to
 *           have zero-padded fractions, which are aligned at the decimal separator. However, this
 *           requires the last digit of the fraction part to be {@code '0'} if the Postgres
 *           convention is desired. </ol>
 * <li> Consecutive unrecognized characters are interpreted as a <b><em>literal</em></b>. It is
 *      also possible to specify a literal by enclosing zero or more characters within double
 *      quotes. If the format string ends before an opening quote is paired, a closing quote is
 *      assumed just after the last character. If a double quote is to be printed, it must be
 *      escaped with a leading backslash. In general, escaping a character causes it to lose its
 *      special meaning if any. In numeric formats, literals are anchored by default. To fix its
 *      position, a literal should be prepended with an {@code F} pattern, e.g. F$, F"USD". </ol>
 */
@SuppressWarnings({"checkstyle:BooleanExpressionComplexity", "checkstyle:CyclomaticComplexity",
        "checkstyle:ExecutableStatementCount", "checkstyle:MagicNumber", "checkstyle:MethodLength",
        "checkstyle:NestedIfDepth", "checkstyle:NPathComplexity"})
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

    public static Formatter forDates(@Nonnull String format) {
        return new DateFormat(format);
    }
    public static Formatter forNumbers(@Nonnull String format) {
        return new NumberFormat(format);
    }

    public abstract String format(@Nonnull Object input, @Nonnull Locale locale);

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

    private static class DateFormat extends Formatter {
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
            final boolean padding;
            final PatternElement pattern;
            final Ordinal ordinal;

            PatternInstance(boolean padding, PatternElement pattern, Ordinal ordinal) {
                this.padding = padding;
                this.pattern = pattern;
                this.ordinal = ordinal;
            }

            @Override
            public void format(StringBuilder s, Temporal input, Locale locale) {
                Object result = pattern.query.apply(input, locale);
                String r = result.toString();
                if (pattern == PatternElement.TZH) {
                    s.append((int) result < 0 ? '-' : '+');
                    r = (int) result < 0 ? r.substring(1) : r;
                }
                if (padding) {
                    char pad = result instanceof Number ? '0' : ' ';
                    for (int i = r.length(); i < pattern.maxLength; i++) {
                        s.append(pad);
                    }
                }
                s.append(r);
                if (pattern == PatternElement.Y_YYY && (padding || r.length() == 4)) {
                    s.insert(s.length() - 3, ',');
                }
                if (ordinal != null) {
                    String th = getOrdinal(r);
                    s.append(ordinal == Ordinal.TH ? th.toUpperCase() : th);
                }
            }

            @Override
            public String toString() {
                return pattern + (ordinal == null ? "" : ordinal.toString());
            }
        }

        @SuppressWarnings("checkstyle:MethodParamPad")
        enum PatternElement {
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
            OF    ((t, l) -> ZoneOffset.from(t).getId(), 6);

            final BiFunction<Temporal, Locale, Object> query;
            final int maxLength;

            PatternElement(PatternElement pattern) {
                this(pattern.query, pattern.maxLength);
            }
            PatternElement(TemporalField field, int maxLength) {
                this((t, l) -> t.get(field), maxLength);
            }
            PatternElement(PatternElement pattern, BiFunction<Object, Locale, Object> transform) {
                this(pattern, transform, pattern.maxLength);
            }
            PatternElement(PatternElement pattern, BiFunction<Object, Locale, Object> transform, int maxLength) {
                this((t, l) -> transform.apply(pattern.query.apply(t, l), l), maxLength);
            }
            PatternElement(BiFunction<Temporal, Locale, Object> query, int maxLength) {
                this.query = query;
                this.maxLength = maxLength;
            }
        }

        enum Ordinal { TH, th }

        class DateTimeGroupProcessor implements GroupProcessor {
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
                    String pattern = m.group(2).replace('.', '_').replace(',', '_');
                    String suffix = m.group(3);
                    parts.add(new PatternInstance(!prefix.contains("FM"), valueOf(PatternElement.class, pattern),
                            suffix == null ? null : Ordinal.valueOf(suffix)));
                }
            }
        }

        DateFormat(String format) {
            parse(DATETIME_TEMPLATE, new DateTimeGroupProcessor(), format);
        }

        @Override
        public String format(@Nonnull Object input, @Nonnull Locale locale) {
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
     * because of the following reasons. <ol>
     * <li> {@link DecimalFormat} does not fill the pattern space with #'s when the integer part
     *      overflows. Instead, it truncates the integer part, which cannot be checked without
     *      obtaining its decimal representation.
     * <li> {@link DecimalFormat} does not support multiple grouping sizes and separators.
     * <li> {@link DecimalFormat} rounds the fractional part using its binary representation, which
     *      produces wrong results even for simple cases such as (0.15 → #.# → 0.1) in {@link
     *      RoundingMode#HALF_UP}.
     * <li> {@link java.util.Formatter} does not cache or expose the intermediate representation of
     *      the format string.
     * <li> {@link java.util.Formatter} can correctly handle rounding by leveraging {@link
     *      sun.misc.FormattedFloatingDecimal} for {@link Float} and {@link Double}, and by using
     *      {@link BigDecimal#BigDecimal(BigInteger, int, MathContext) BigDecimal(BigInteger,
     *      scale, precision)}. Both {@link sun.misc.FormattedFloatingDecimal} and {@link
     *      BigDecimal} are using the decimal representation of floating point numbers, but for the
     *      former, the rounding mode is not configurable. </ol>
     */
    private static class NumberFormat extends Formatter {
        private final Form form;
        private final Mask integerMask;
        private final Mask fractionMask;
        private final boolean padding;
        private final boolean currency;
        private final int shift;

        enum Form { Normal, Exponential, Roman }

        interface Anchorable {
            boolean isAnchored();
        }

        enum PatternElement implements Anchorable {
            BR, B, SG, S, MI, M, PL, P, CR, C, TH, th, EEEE, eeee, RN, rn;

            static final List<PatternElement> SIGN = Arrays.asList(BR, B, SG, S, MI, M, PL, P);
            static final List<PatternElement> NEGATIVE = Arrays.asList(BR, B, SG, S, MI, M);
            static final List<PatternElement> ANCHORED = Arrays.asList(B, S, M, P, C, TH, th, EEEE, eeee, RN, rn);

            boolean isSign() {
                return SIGN.contains(this);
            }
            boolean isNegativeSign() {
                return NEGATIVE.contains(this);
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
         * Stores {@link Literal}, {@link PatternElement}, {@link Character} and {@link Integer}
         * to represent arbitrary text, sign/currency/ordinals/exponent/roman numerals,
         * decimal/grouping separators, and digit groups respectively.
         */
        class Mask {
            final boolean pre;
            final List<Object> groups;
            final int digits;
            final int minDigits;

            int offerSign = -1;
            boolean negative;
            PatternElement bracket;

            Mask(boolean pre, List<Object> groups, StringBuilder digitMask, int minDigits) {
                this.pre = pre;
                this.groups = new ArrayList<>(groups);

                if (pre) {
                    Collections.reverse(this.groups);
                    digitMask.reverse();
                }
                for (int i = 0; i < this.groups.size(); i++) {
                    Object g = this.groups.get(i);
                    if (!(g instanceof Anchorable) || ((Anchorable) g).isAnchored()) {
                        offerSign = i;
                    }
                    if (g instanceof PatternElement && ((PatternElement) g).isNegativeSign()) {
                        negative = true;
                        if (g == PatternElement.BR || g == PatternElement.B) {
                            bracket = (PatternElement) g;
                        }
                    }
                }
                offerSign++;
                digits = digitMask.length();
                this.minDigits = Math.max(digitMask.lastIndexOf("0") + 1, minDigits);
            }

            /** Ensure matching bracket. Add anchored minus if there is no negative sign provision. */
            void ensureSignProvision(Mask pair) {
                PatternElement inferred = bracket == null && pair.bracket != null ? pair.bracket
                        : pre && !(negative | pair.negative) && digits + pair.digits > 0
                                && form != Form.Roman ? PatternElement.M : null;
                if (inferred != null) {
                    groups.add(offerSign, inferred);
                }
            }

            void format(StringBuilder s, boolean negative, String integer, String fraction, int exponent,
                        boolean overflow, DecimalFormatSymbols symbols) {
                StringBuilder digits = overflow ? null : pre ? new StringBuilder(integer).reverse()
                        : new StringBuilder(fraction);
                if (!overflow) {
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
                // f: Accumulate the padding space until a fixed element or the end of the mask is encountered
                for (int i = 0, d = 0, f = 0; i < groups.size();) {
                    Object g = groups.get(i);
                    if (g instanceof Literal) {
                        parts.add(((Literal) g).contents);
                    } else if (g instanceof PatternElement) {
                        PatternElement p = (PatternElement) g;
                        if (p.isSign()) {
                            char c = p.getSign(pre, negative);
                            if (c != ' ') {
                                parts.add(c);
                            } else if (padding) {
                                f++;
                            }
                        } else if (p == PatternElement.CR || p == PatternElement.C) {
                            parts.add(symbols.getCurrencySymbol());
                        } else if (p == PatternElement.TH || p == PatternElement.th) {
                            if (integer != null && !integer.isEmpty()) {
                                String th = getOrdinal(integer);
                                parts.add(p == PatternElement.TH ? th.toUpperCase() : th);
                            } else if (padding) {
                                f += 2;
                            }
                        } else if (p == PatternElement.EEEE || p == PatternElement.eeee) {
                            StringBuilder r = new StringBuilder();
                            String e = symbols.getExponentSeparator();
                            r.append(p == PatternElement.EEEE ? e : e.toLowerCase());
                            r.append(exponent < 0 ? '-' : '+');
                            if (overflow) {
                                r.append("##");
                            } else {
                                exponent = Math.abs(exponent);
                                (exponent <= 9 ? r.append('0') : r).append(exponent);
                            }
                            parts.add(r);
                        } else if (p == PatternElement.RN || p == PatternElement.rn) {
                            int n = integer == null || integer.length() > 4 ? Integer.MAX_VALUE
                                    : integer.isEmpty() ? 0 : Integer.parseInt(integer);
                            if (form == Form.Roman || n != 0) {
                                String r = toRoman(form == Form.Roman && (n == 0 || negative)
                                        ? Integer.MAX_VALUE : n);
                                parts.add(p == PatternElement.rn ? r.toLowerCase() : r);
                                if (padding) {
                                    f += 15 - r.length();
                                }
                            } else if (padding) {
                                f += 15;
                            }
                        }
                    } else if (g instanceof Character) {
                        if (overflow || d < digits.length()) {
                            parts.add(g.equals('G') ? symbols.getGroupingSeparator() : g.equals('D') ? (currency
                                    ? symbols.getMonetaryDecimalSeparator() : symbols.getDecimalSeparator()) : g);
                        } else if (padding) {
                            f++;
                        }
                    } else if (g instanceof Integer) {
                        if (overflow) {
                            StringBuilder r = new StringBuilder();
                            for (int j = 0; j < (int) g; j++) {
                                r.append('#');
                            }
                            parts.add(r);
                        } else if (d < digits.length()) {
                            int e = Math.min(d + (int) g, digits.length());
                            StringBuilder r = new StringBuilder().append(digits, d, e);
                            parts.add(pre ? r.reverse() : r);
                            if (padding) {
                                f += d + (int) g - e;
                            }
                        } else if (padding) {
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

        static class NumericGroupProcessor implements GroupProcessor {
            final List<Object> groups = new ArrayList<>();
            final StringBuilder integerDigits = new StringBuilder();
            final StringBuilder fractionDigits = new StringBuilder();
            int decimalSeparator = -1;
            int afterLastDigit = -1;
            boolean fillMode;
            boolean currency;
            boolean exponential;
            boolean roman;
            int shift;

            @Override
            public void acceptLiteral(String literal) {
                add(new Literal(literal));
            }

            @Override
            public void acceptGroup(String group, Matcher m) {
                String g = group.toUpperCase();
                if (g.equals("FM")) {
                    fillMode = true;
                } else if (g.startsWith("V")) {
                    shift += group.length() - 1;
                } else if (g.startsWith("F") || g.startsWith("\"")) {
                    add(new Literal(group));
                } else if (g.startsWith("9") || g.startsWith("0")) {
                    (decimalSeparator == -1 ? integerDigits : fractionDigits).append(group);
                    add(group.length());
                    afterLastDigit = groups.size();
                } else if (g.length() == 1 && ",G.D".contains(g)) {
                    if (".D".contains(g) && decimalSeparator == -1) {
                        decimalSeparator = groups.size();
                    }
                    add(g.charAt(0));
                } else {
                    PatternElement pattern = valueOf(PatternElement.class, group);
                    add(pattern);
                    if (pattern == PatternElement.CR || pattern == PatternElement.C) {
                        currency = true;
                    } else if (pattern == PatternElement.EEEE || pattern == PatternElement.eeee) {
                        exponential = true;
                    } else if (pattern == PatternElement.RN || pattern == PatternElement.rn) {
                        roman = true;
                    }
                }
            }

            void add(Object group) {
                groups.add(group);
            }
        }

        NumberFormat(String format) {
            NumericGroupProcessor p = new NumericGroupProcessor();
            parse(NUMERIC_TEMPLATE, p, format);
            int split = p.decimalSeparator != -1 ? p.decimalSeparator
                    : p.afterLastDigit != -1 ? p.afterLastDigit : p.groups.size();
            boolean zero = p.integerDigits.indexOf("0") != -1 || p.fractionDigits.indexOf("0") != -1;
            integerMask = new Mask(true, p.groups.subList(0, split), p.integerDigits,
                    !zero && p.integerDigits.length() > 0 ? 1 : 0);
            fractionMask = new Mask(false, p.groups.subList(split, p.groups.size()),
                    p.fractionDigits, !zero && p.integerDigits.length() == 0 ? 1 : 0);

            form = p.roman && integerMask.digits == 0 && fractionMask.digits == 0 ? Form.Roman
                    : p.exponential ? Form.Exponential : Form.Normal;
            padding = !p.fillMode;
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
         *      <li> is greater than the length of {@code digits}, the floating-point (.)
         *           underflows and the integer part is appended with zeros.
         *      <li> is less than zero, the floating-point (.) overflows and the fraction part is
         *           prepended with zeros.
         *      <li> is in the range [0, {@code digits.length()}], the integer and fraction parts
         *           are split normally by the floating-point (.). </ol>
         *      In all cases, neither the integer nor fraction parts contain excess digits. This is
         *      actually an optimization against very low negative exponents.
         * <li> Format the integer and fraction masks. If the length of the integer part is greater
         *      than the number of integer digits in the pattern, the pattern is filled with hashes
         *      (#). </ol>
         * Useful notes: <ol>
         * <li> Floating-point numbers are always stored and represented in the exponential form.
         *      When the exponent is omitted, the floating-point of the number is aligned to the
         *      decimal separator in the pattern, so there may be overflow/underflow. When the
         *      exponent is printed, the number is left-aligned to the pattern, and the exponent is
         *      updated accordingly. </ol>
         */
        @Override
        public String format(@Nonnull Object input, @Nonnull Locale locale) {
            if (!(input instanceof Number)) {
                throw QueryException.dataException("Input parameter is expected to be numeric");
            }
            DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(locale);
            StringBuilder s = new StringBuilder();
            String value = input.toString();
            boolean negative = value.startsWith("-");

            if (value.equals("NaN") || value.endsWith("Infinity")) {
                // Value is not formattable; pattern is filled with #'s.
                integerMask.format(s, negative, null, null, 0, true, symbols);
                fractionMask.format(s, negative, null, null, 0, true, symbols);
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
                if (!digits.isEmpty()) {
                    exponent += digits.length() - integerLength;
                }

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
                    integerLength = 0;
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
                // If integer part overflows; pattern is filled with #'s.
                boolean overflow = form != Form.Roman && integerLength > integerMask.digits;
                integerMask.format(s, negative, integer, fraction, exponent, overflow, symbols);
                fractionMask.format(s, negative, integer, fraction, exponent, overflow, symbols);
            }
            return s.toString();
        }

        @Override
        public String toString() {
            return integerMask.toString() + fractionMask.toString();
        }
    }
}
