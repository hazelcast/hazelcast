/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query;

import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.BetweenPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.GreaterLessPredicate;
import com.hazelcast.query.impl.predicates.ILikePredicate;
import com.hazelcast.query.impl.predicates.InPredicate;
import com.hazelcast.query.impl.predicates.InstanceOfPredicate;
import com.hazelcast.query.impl.predicates.LikePredicate;
import com.hazelcast.query.impl.predicates.NotEqualPredicate;
import com.hazelcast.query.impl.predicates.NotPredicate;
import com.hazelcast.query.impl.predicates.OrPredicate;
import com.hazelcast.query.impl.predicates.RegexPredicate;

import java.util.Date;

/**
 * A utility class to create {@link com.hazelcast.query.Predicate} instances.
 * <p>
 * <b>Special Attributes</b>
 * <p>
 * The predicate factory methods accepting an attribute name support two special attributes:
 * <ul>
 * <li>{@link QueryConstants#KEY_ATTRIBUTE_NAME "__key"} - instructs the predicate to act on the key associated
 * with an item.
 * <li>{@link QueryConstants#THIS_ATTRIBUTE_NAME "this"} - instructs the predicate to act on the value associated
 * with an item.
 * </ul>
 * <p>
 * <b>Attribute Paths</b>
 * <p>
 * Dot notation may be used for attribute name to instruct the predicate to act on the attribute located at deeper
 * level of an item: given {@code "fullName.firstName"} path the predicate will act on {@code firstName} attribute
 * of the value fetched by {@code fullName} attribute from the item itself. If any of the attributes along the path
 * can't be resolved, {@link IllegalArgumentException} will be thrown. Reading of any attribute from {@code null}
 * will produce {@code null} value.
 * <p>
 * Square brackets notation may be used to instruct the predicate to act on the array/collection element at the
 * specified index: given {@code "names[0]"} path the predicate will act on the first item of the array/collection
 * fetched by {@code names} attribute from the item. The index must be non-negative, otherwise
 * {@link IllegalArgumentException} will be thrown. Reading from the index pointing beyond the end of the collection/array
 * will produce {@code null} value.
 * <p>
 * Special {@code any} keyword may be used to act on every array/collection element: given
 * {@code "names[any].fullName.firstName"} path the predicate will act on {@code firstName} attribute of the value
 * fetched by {@code fullName} attribute from every array/collection element stored in the item itself under {@code names}
 * attribute.
 * <p>
 * <b>Handling of {@code null}</b>
 * <p>
 * The predicate factory methods can accept {@code null} as a value to compare with or a pattern to match against
 * if and only if that is explicitly stated in the method documentation. In this case, the usual {@code null} equality
 * logic applies: if {@code null} is provided, the predicate passes an item if and only if the value stored under the
 * item attribute in question is also {@code null}.
 * <p>
 * Special care must be taken while comparing with {@code null} values <i>stored</i> inside items being filtered
 * through the predicates created by the following methods: {@link #greaterThan}, {@link #greaterEqual}, {@link #lessThan},
 * {@link #lessEqual}, {@link #between}. The predicates produced by these methods intentionally violate {@link Comparable}
 * contract by not throwing {@link NullPointerException} for {@code null} values. Instead, they always evaluate to
 * {@code false} and therefore never pass such items.
 * <p>
 * <b>Implicit Type Conversion</b>
 * <p>
 * If the type of the stored value doesn't match the type of the value provided to the predicate, implicit type conversion
 * is performed before predicate evaluation. The provided value is converted to match the type of the stored attribute value.
 * If no conversion matching the type exists, {@link IllegalArgumentException} is thrown.
 * <p>
 * Depending on the attribute type following conversions may apply:
 * <ul>
 * <li>{@link Number Numeric types}
 * <ul>
 * <li>Strings are parsed in attempt to extract the represented numeric value from them in the same way as
 * {@link Integer#parseInt(String)} and analogous methods of other types do. Strings containing no meaningful
 * representation will produce {@link NumberFormatException}.
 * <li>Widening conversion may be performed in the same way as described in JLS 5.1.2 Widening Primitive Conversions.
 * <li>Narrowing conversion may be performed in the same way as described in JLS 5.1.3 Narrowing Primitive Conversions.
 * </ul>
 * <li>{@link Boolean} type
 * <ul>
 * <li>A string that case-insensitively equals to {@code "true"} is converted to {@code true}, all other strings are
 * converted to {@code false}.
 * <li>Any non-zero numeric value is converted to {@code true}, the zero is converted to {@code false}.
 * </ul>
 * <li>String type
 * <ul>
 * <li>{@link Object#toString()} is invoked to produce the string representation for non-string values.
 * </ul>
 * <li>{@link Character} type
 * <ul>
 * <li>The first character of a string is used for the conversion. Empty strings are not allowed and will produce
 * {@link IllegalArgumentException}.
 * <li>Any numeric value is converted to an integer value representing a single UTF-16 code unit to create a character
 * from. This process may involve widening and narrowing conversions as described in JLS 5.1.2 Widening Primitive
 * Conversions and 5.1.3 Narrowing Primitive Conversions.
 * </ul>
 * <li>Enum types
 * <ul>
 * <li>Any non-string value is converted to a string using {@link Object#toString()}, which is interpreted as a
 * case-sensitive enum member name.
 * </ul>
 * <li>{@link java.math.BigInteger BigInteger} type
 * <ul>
 * <li>Numeric values are converted to {@link java.math.BigInteger BigInteger} by applying widening or narrowing
 * conversion as described in JLS 5.1.2 Widening Primitive Conversions and 5.1.3 Narrowing Primitive Conversions.
 * <li>Boolean {@code true} and {@code false} are converted to {@link java.math.BigInteger#ONE BigInteger.ONE}
 * and {@link java.math.BigInteger#ZERO BigInteger.ZERO} respectively.
 * <li>A value of any other type is converted to string using {@link Object#toString()}, which is interpreted as
 * a base 10 representation in the same way as {@link java.math.BigInteger#BigInteger(String) BigInteger(String)} does.
 * If the representation is invalid, {@link NumberFormatException} will be thrown.
 * </ul>
 * <li>{@link java.math.BigDecimal BigDecimal} type
 * <ul>
 * <li>Numeric value are converted to {@link java.math.BigDecimal BigDecimal} by applying widening conversion as
 * described in JLS 5.1.2 Widening Primitive Conversions.
 * <li>Boolean {@code true} and {@code false} are converted to {@link java.math.BigDecimal#ONE BigDecimal.ONE}
 * and {@link java.math.BigDecimal#ZERO BigDecimal.ZERO} respectively.
 * <li>A value of any other type is converted to string using {@link Object#toString()}, which is interpreted as
 * a base 10 representation in the same way as {@link java.math.BigDecimal#BigDecimal(String) BigDecimal(String)} does.
 * If the representation is invalid, {@link NumberFormatException} will be thrown.
 * </ul>
 * <li>{@link java.sql.Timestamp SQL Timestamp} type
 * <ul>
 * <li>Date values are converted to {@link java.sql.Timestamp SQL Timestamp} by applying {@link Date#getTime()} on them.
 * <li>String values are interpreted in the same way as {@link java.sql.Timestamp#valueOf(String)} does.
 * {@link RuntimeException} wrapping {@link java.text.ParseException} is thrown for invalid representations.
 * <li>Any numeric value is interpreted as the number of milliseconds since <i>January 1, 1970, 00:00:00 GMT</i> by
 * applying widening or narrowing conversion as described in JLS 5.1.2 Widening Primitive Conversions and 5.1.3
 * Narrowing Primitive Conversions.
 * </ul>
 * <li>{@link java.sql.Date SQL Date} type
 * <ul>
 * <li>String values are interpreted in the same way as {@link java.sql.Date#valueOf(String)} does.
 * {@link RuntimeException} wrapping {@link java.text.ParseException} is thrown for invalid representations.
 * <li>Any numeric value is interpreted as the number of milliseconds since <i>January 1, 1970, 00:00:00 GMT</i>
 * by applying widening or narrowing conversion as described in JLS 5.1.2 Widening Primitive Conversions and
 * 5.1.3 Narrowing Primitive Conversions.
 * </ul>
 * <li>{@link Date} type
 * <ul>
 * <li>String values are interpreted as having <i>EEE MMM dd HH:mm:ss zzz yyyy</i> format and
 * {@link java.util.Locale#US US} locale in the same way as {@link java.text.SimpleDateFormat} does.
 * {@link RuntimeException} wrapping {@link java.text.ParseException} is thrown for invalid representations.
 * <li>Any numeric value is interpreted as the number of milliseconds since <i>January 1, 1970, 00:00:00 GMT</i> by
 * applying widening or narrowing conversion as described in JLS 5.1.2 Widening Primitive Conversions and 5.1.3
 * Narrowing Primitive Conversions.
 * </ul>
 * <li>{@link java.util.UUID UUID} type
 * <ul>
 * <li>String values are interpreted in the same way as {@link java.util.UUID#fromString(String)} does.
 * {@link IllegalArgumentException} is thrown for invalid representations.
 * </ul>
 * </ul>
 */
public final class Predicates {

    //we don't want instances. private constructor.
    private Predicates() {
    }

    /**
     * Creates an <b>instance of</b> predicate that will pass items which are {@code instanceof} the given {@code klass}.
     *
     * @param klass the class the created predicate will check for.
     * @return the created <b>instance of</b> predicate.
     */
    public static Predicate instanceOf(final Class klass) {
        return new InstanceOfPredicate(klass);
    }

    /**
     * Creates an <b>and</b> predicate that will perform the logical <b>and</b> operation on the given {@code predicates}.
     * <p>
     * If the given {@code predicates} list is empty, the created predicate will always evaluate to {@code true}
     * and will pass any item.
     *
     * @param predicates the child predicates to form the resulting <b>and</b> predicate from.
     * @return the created <b>and</b> predicate instance.
     */
    public static Predicate and(Predicate... predicates) {
        return new AndPredicate(predicates);
    }

    /**
     * Creates a <b>not</b> predicate that will negate the result of the given {@code predicate}.
     *
     * @param predicate the predicate to negate the value of.
     * @return the created <b>not</b> predicate instance.
     */
    public static Predicate not(Predicate predicate) {
        return new NotPredicate(predicate);
    }

    /**
     * Creates an <b>or</b> predicate that will perform the logical <b>or</b> operation on the given {@code predicates}.
     * <p>
     * If the given {@code predicates} list is empty, the created predicate will always evaluate to {@code false}
     * and will never pass any items.
     *
     * @param predicates the child predicates to form the resulting <b>or</b> predicate from.
     * @return the created <b>or</b> predicate instance.
     */
    public static Predicate or(Predicate... predicates) {
        return new OrPredicate(predicates);
    }

    /**
     * Creates a <b>not equal</b> predicate that will pass items if the given {@code value} and the value stored under
     * the given item {@code attribute} are not equal.
     * <p>
     * See also <i>Special Attributes</i>, <i>Attribute Paths</i>, <i>Handling of {@code null}</i> and
     * <i>Implicit Type Conversion</i> sections of {@link Predicates}.
     *
     * @param attribute the attribute to fetch the value for comparison from.
     * @param value     the value to compare the attribute value against. Can be {@code null}.
     * @return the created <b>not equal</b> predicate instance.
     * @throws IllegalArgumentException if the {@code attribute} does not exist.
     */
    public static Predicate notEqual(String attribute, Comparable value) {
        return new NotEqualPredicate(attribute, value);
    }

    /**
     * Creates an <b>equal</b> predicate that will pass items if the given {@code value} and the value stored under
     * the given item {@code attribute} are equal.
     * <p>
     * See also <i>Special Attributes</i>, <i>Attribute Paths</i>, <i>Handling of {@code null}</i> and
     * <i>Implicit Type Conversion</i> sections of {@link Predicates}.
     *
     * @param attribute the attribute to fetch the value for comparison from.
     * @param value     the value to compare the attribute value against. Can be {@code null}.
     * @return the created <b>equal</b> predicate instance.
     * @throws IllegalArgumentException if the {@code attribute} does not exist.
     */
    public static Predicate equal(String attribute, Comparable value) {
        return new EqualPredicate(attribute, value);
    }

    /**
     * Creates a <b>like</b> predicate that will pass items if the given {@code pattern} matches the value stored under
     * the given item {@code attribute}.
     * <p>
     * See also <i>Special Attributes</i>, <i>Attribute Paths</i> and <i>Handling of {@code null}</i> sections of
     * {@link Predicates}.
     *
     * @param attribute the attribute to fetch the value for matching from.
     * @param pattern   the pattern to match the attribute value against. The % (percentage sign) is a placeholder for
     *                  multiple characters, the _ (underscore) is a placeholder for a single character. If you need to
     *                  match the percentage sign or the underscore character itself, escape it with the backslash,
     *                  for example {@code "\\%"} string will match the percentage sign. Can be {@code null}.
     * @return the created <b>like</b> predicate instance.
     * @throws IllegalArgumentException if the {@code attribute} does not exist.
     * @see #ilike(String, String)
     * @see #regex(String, String)
     */
    public static Predicate like(String attribute, String pattern) {
        return new LikePredicate(attribute, pattern);
    }

    /**
     * Creates a <b>case-insensitive like</b> predicate that will pass items if the given {@code pattern} matches the value
     * stored under the given item {@code attribute} in a case-insensitive manner.
     * <p>
     * See also <i>Special Attributes</i>, <i>Attribute Paths</i> and <i>Handling of {@code null}</i> sections of
     * {@link Predicates}.
     *
     * @param attribute the attribute to fetch the value for matching from.
     * @param pattern   the pattern to match the attribute value against. The % (percentage sign) is a placeholder for
     *                  multiple characters, the _ (underscore) is a placeholder for a single character. If you need to
     *                  match the percentage sign or the underscore character itself, escape it with the backslash,
     *                  for example {@code "\\%"} string will match the percentage sign. Can be {@code null}.
     * @return the created <b>case-insensitive like</b> predicate instance.
     * @throws IllegalArgumentException if the {@code attribute} does not exist.
     * @see #like(String, String)
     * @see #regex(String, String)
     */
    public static Predicate ilike(String attribute, String pattern) {
        return new ILikePredicate(attribute, pattern);
    }

    /**
     * Creates a <b>regex</b> predicate that will pass items if the given {@code pattern} matches the value stored under
     * the given item {@code attribute}.
     * <p>
     * See also <i>Special Attributes</i>, <i>Attribute Paths</i> and <i>Handling of {@code null}</i> sections of
     * {@link Predicates}.
     *
     * @param attribute the attribute to fetch the value for matching from.
     * @param pattern   the pattern to match the attribute value against. The pattern interpreted exactly the same as
     *                  described in {@link java.util.regex.Pattern}. Can be {@code null}.
     * @return the created <b>regex</b> predicate instance.
     * @throws IllegalArgumentException if the {@code attribute} does not exist.
     * @see #like(String, String)
     * @see #ilike(String, String)
     */
    public static Predicate regex(String attribute, String pattern) {
        return new RegexPredicate(attribute, pattern);
    }

    /**
     * Creates a <b>greater than</b> predicate that will pass items if the value stored under the given
     * item {@code attribute} is greater than the given {@code value}.
     * <p>
     * See also <i>Special Attributes</i>, <i>Attribute Paths</i>, <i>Handling of {@code null}</i> and
     * <i>Implicit Type Conversion</i> sections of {@link Predicates}.
     *
     * @param attribute the left-hand side attribute to fetch the value for comparison from.
     * @param value     the right-hand side value to compare the attribute value against.
     * @return the created <b>greater than</b> predicate.
     * @throws IllegalArgumentException if the {@code attribute} does not exist.
     */
    public static Predicate greaterThan(String attribute, Comparable value) {
        return new GreaterLessPredicate(attribute, value, false, false);
    }

    /**
     * Creates a <b>greater than or equal to</b> predicate that will pass items if the value stored under the given
     * item {@code attribute} is greater than or equal to the given {@code value}.
     * <p>
     * See also <i>Special Attributes</i>, <i>Attribute Paths</i>, <i>Handling of {@code null}</i> and
     * <i>Implicit Type Conversion</i> sections of {@link Predicates}.
     *
     * @param attribute the left-hand side attribute to fetch the value for comparison from.
     * @param value     the right-hand side value to compare the attribute value against.
     * @return the created <b>greater than or equal to</b> predicate.
     * @throws IllegalArgumentException if the {@code attribute} does not exist.
     */
    public static Predicate greaterEqual(String attribute, Comparable value) {
        return new GreaterLessPredicate(attribute, value, true, false);
    }

    /**
     * Creates a <b>less than</b> predicate that will pass items if the value stored under the given item {@code attribute}
     * is less than the given {@code value}.
     * <p>
     * See also <i>Special Attributes</i>, <i>Attribute Paths</i>, <i>Handling of {@code null}</i> and
     * <i>Implicit Type Conversion</i> sections of {@link Predicates}.
     *
     * @param attribute the left-hand side attribute to fetch the value for comparison from.
     * @param value     the right-hand side value to compare the attribute value against.
     * @return the created <b>less than</b> predicate.
     * @throws IllegalArgumentException if the {@code attribute} does not exist.
     */
    public static Predicate lessThan(String attribute, Comparable value) {
        return new GreaterLessPredicate(attribute, value, false, true);
    }

    /**
     * Creates a <b>less than or equal to</b> predicate that will pass items if the value stored under the given
     * item {@code attribute} is less than or equal to the given {@code value}.
     * <p>
     * See also <i>Special Attributes</i>, <i>Attribute Paths</i>, <i>Handling of {@code null}</i> and
     * <i>Implicit Type Conversion</i> sections of {@link Predicates}.
     *
     * @param attribute the left-hand side attribute to fetch the value for comparison from.
     * @param value     the right-hand side value to compare the attribute value against.
     * @return the created <b>less than or equal to</b> predicate.
     * @throws IllegalArgumentException if the {@code attribute} does not exist.
     */
    public static Predicate lessEqual(String attribute, Comparable value) {
        return new GreaterLessPredicate(attribute, value, true, true);
    }

    /**
     * Creates a <b>between</b> predicate that will pass items if the value stored under the given item {@code attribute}
     * is contained inside the given range. The range begins at the given {@code from} bound and ends at
     * the given {@code to} bound. The bounds are <b>inclusive</b>.
     * <p>
     * See also <i>Special Attributes</i>, <i>Attribute Paths</i>, <i>Handling of {@code null}</i> and
     * <i>Implicit Type Conversion</i> sections of {@link Predicates}.
     *
     * @param attribute the attribute to fetch the value to check from.
     * @param from      the inclusive lower bound of the range to check.
     * @param to        the inclusive upper bound of the range to check.
     * @return the created <b>between</b> predicate.
     * @throws IllegalArgumentException if the {@code attribute} does not exist.
     */
    public static Predicate between(String attribute, Comparable from, Comparable to) {
        return new BetweenPredicate(attribute, from, to);
    }

    /**
     * Creates a <b>in</b> predicate that will pass items if the value stored under the given item {@code attribute}
     * is a member of the given {@code values} set.
     * <p>
     * See also <i>Special Attributes</i>, <i>Attribute Paths</i>, <i>Handling of {@code null}</i> and
     * <i>Implicit Type Conversion</i> sections of {@link Predicates}.
     *
     * @param attribute the attribute to fetch the value to test from.
     * @param values    the values set to test the membership in. Individual values can be {@code null}.
     * @return the created <b>in</b> predicate.
     * @throws IllegalArgumentException if the {@code attribute} does not exist.
     */
    public static Predicate in(String attribute, Comparable... values) {
        return new InPredicate(attribute, values);
    }

}
