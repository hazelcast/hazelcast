/*
 * Copyright 2021 Hazelcast Inc.
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
package org.apache.calcite.linq4j.tree;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Represents an expression that has a constant value.
 */
// A copy of actual Calcite class patched with https://github.com/apache/calcite/pull/2530
// TODO: Remove when Calcite is released with the bug fixed
@SuppressWarnings("all")
public class ConstantExpression extends Expression {
  public final @Nullable Object value;

  public ConstantExpression(Type type, @Nullable Object value) {
    super(ExpressionType.Constant, type);
    this.value = value;
    if (value != null) {
      if (type instanceof Class) {
        Class clazz = (Class) type;
        clazz = Primitive.box(clazz);
        if (!clazz.isInstance(value)
            && !((clazz == Float.class || clazz == Double.class)
                && value instanceof BigDecimal)) {
          throw new AssertionError(
              "value " + value + " does not match type " + type);
        }
      }
    }
  }

  @Override public @Nullable Object evaluate(Evaluator evaluator) {
    return value;
  }

  @Override public Expression accept(Shuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override void accept(ExpressionWriter writer, int lprec, int rprec) {
    if (value == null) {
      if (!writer.requireParentheses(this, lprec, rprec)) {
        writer.append("(").append(type).append(") null");
      }
      return;
    }
    write(writer, value, type);
  }

  private static ExpressionWriter write(ExpressionWriter writer,
      final Object value, @Nullable Type type) {
    if (value == null) {
      return writer.append("null");
    }
    if (type == null) {
      type = value.getClass();
      type = Primitive.unbox(type);
    }
    if (value instanceof String) {
      escapeString(writer.getBuf(), (String) value);
      return writer;
    }
    final Primitive primitive = Primitive.of(type);
    final BigDecimal bigDecimal;
    if (primitive != null) {
      switch (primitive) {
      case BYTE:
        return writer.append("(byte)").append(((Byte) value).intValue());
      case CHAR:
        return writer.append("(char)").append((int) (Character) value);
      case SHORT:
        return writer.append("(short)").append(((Short) value).intValue());
      case LONG:
        return writer.append(value).append("L");
      case FLOAT:
        if (value instanceof BigDecimal) {
          bigDecimal = (BigDecimal) value;
        } else {
          bigDecimal = BigDecimal.valueOf((Float) value);
        }
        if (bigDecimal.precision() > 6) {
          return writer.append("Float.intBitsToFloat(")
              .append(Float.floatToIntBits(bigDecimal.floatValue()))
              .append(")");
        }
        return writer.append(value).append("F");
      case DOUBLE:
        if (value instanceof BigDecimal) {
          bigDecimal = (BigDecimal) value;
        } else {
          bigDecimal = BigDecimal.valueOf((Double) value);
        }
        if (bigDecimal.precision() > 10) {
          return writer.append("Double.longBitsToDouble(")
              .append(Double.doubleToLongBits(bigDecimal.doubleValue()))
              .append("L)");
        }
        return writer.append(value).append("D");
      default:
        return writer.append(value);
      }
    }
    final Primitive primitive2 = Primitive.ofBox(type);
    if (primitive2 != null) {
      writer.append(primitive2.boxName + ".valueOf(");
      write(writer, value, primitive2.primitiveClass);
      return writer.append(")");
    }
    // ----- PATCHED -----
    Primitive primitive3 = Primitive.ofBox(value.getClass());
    if (Object.class.equals(type) && primitive3 != null) {
      return write(writer, value, primitive3.primitiveClass);
    }
    // -------------------
    if (value instanceof Enum) {
      return writer.append(((Enum) value).getDeclaringClass())
          .append('.')
          .append(((Enum) value).name());
    }
    if (value instanceof BigDecimal) {
      bigDecimal = ((BigDecimal) value).stripTrailingZeros();
      try {
        final int scale = bigDecimal.scale();
        final long exact = bigDecimal.scaleByPowerOfTen(scale).longValueExact();
        writer.append("java.math.BigDecimal.valueOf(").append(exact).append("L");
        if (scale != 0) {
          writer.append(", ").append(scale);
        }
        return writer.append(")");
      } catch (ArithmeticException e) {
        return writer.append("new java.math.BigDecimal(\"")
            .append(bigDecimal.toString()).append("\")");
      }
    }
    if (value instanceof BigInteger) {
      BigInteger bigInteger = (BigInteger) value;
      return writer.append("new java.math.BigInteger(\"")
          .append(bigInteger.toString()).append("\")");
    }
    if (value instanceof Class) {
      Class clazz = (Class) value;
      return writer.append(clazz.getCanonicalName()).append(".class");
    }
    if (value instanceof Types.RecordType) {
      final Types.RecordType recordType = (Types.RecordType) value;
      return writer.append(recordType.getName()).append(".class");
    }
    if (value.getClass().isArray()) {
      writer.append("new ").append(requireNonNull(value.getClass().getComponentType()));
      list(writer, Primitive.asList(value), "[] {\n", ",\n", "}");
      return writer;
    }
    if (value instanceof List) {
      if (((List) value).isEmpty()) {
        writer.append("java.util.Collections.EMPTY_LIST");
        return writer;
      }
      list(writer, (List) value, "java.util.Arrays.asList(", ",\n", ")");
      return writer;
    }
    if (value instanceof Map) {
      return writeMap(writer, (Map) value);
    }
    if (value instanceof Set) {
      return writeSet(writer, (Set) value);
    }

    Constructor constructor = matchingConstructor(value);
    if (constructor != null) {
      writer.append("new ").append(value.getClass());
      list(writer,
          Arrays.stream(value.getClass().getFields())
              // <@Nullable Object> is needed for CheckerFramework
              .<@Nullable Object>map(field -> {
                try {
                  return field.get(value);
                } catch (IllegalAccessException e) {
                  throw new RuntimeException(e);
                }
              })
              .collect(Collectors.toList()),
          "(\n", ",\n", ")");
      return writer;
    }

    return writer.append(value);
  }

  private static void list(ExpressionWriter writer, List list,
      String begin, String sep, String end) {
    writer.begin(begin);
    for (int i = 0; i < list.size(); i++) {
      Object value = list.get(i);
      if (i > 0) {
        writer.append(sep).indent();
      }
      write(writer, value, null);
    }
    writer.end(end);
  }

  private static ExpressionWriter writeMap(ExpressionWriter writer, Map map) {
    writer.append("com.google.common.collect.ImmutableMap.");
    if (map.isEmpty()) {
      return writer.append("of()");
    }
    if (map.size() < 5) {
      return map(writer, map, "of(", ",\n", ")");
    }
    return map(writer, map, "builder().put(", ")\n.put(", ").build()");
  }

  private static ExpressionWriter map(ExpressionWriter writer, Map map,
      String begin, String entrySep, String end) {
    writer.append(begin);
    boolean comma = false;
    for (Object o : map.entrySet()) {
      Map.Entry entry = (Map.Entry) o;
      if (comma) {
        writer.append(entrySep).indent();
      }
      write(writer, entry.getKey(), null);
      writer.append(", ");
      write(writer, entry.getValue(), null);
      comma = true;
    }
    return writer.append(end);
  }

  private static ExpressionWriter writeSet(ExpressionWriter writer, Set set) {
    writer.append("com.google.common.collect.ImmutableSet.");
    if (set.isEmpty()) {
      return writer.append("of()");
    }
    if (set.size() < 5) {
      return set(writer, set, "of(", ",", ")");
    }
    return set(writer, set, "builder().add(", ")\n.add(", ").build()");
  }

  private static ExpressionWriter set(ExpressionWriter writer, Set set,
                                      String begin, String entrySep, String end) {
    writer.append(begin);
    boolean comma = false;
    for (Object o : set.toArray()) {
      if (comma) {
        writer.append(entrySep).indent();
      }
      write(writer, o, null);
      comma = true;
    }
    return writer.append(end);
  }

  private static @Nullable Constructor matchingConstructor(Object value) {
    final Field[] fields = value.getClass().getFields();
    for (Constructor<?> constructor : value.getClass().getConstructors()) {
      if (argsMatchFields(fields, constructor.getParameterTypes())) {
        return constructor;
      }
    }
    return null;
  }

  private static boolean argsMatchFields(Field[] fields,
      Class<?>[] parameterTypes) {
    if (parameterTypes.length != fields.length) {
      return false;
    }
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].getType() != parameterTypes[i]) {
        return false;
      }
    }
    return true;
  }

  private static void escapeString(StringBuilder buf, String s) {
    buf.append('"');
    int n = s.length();
    char lastChar = 0;
    for (int i = 0; i < n; ++i) {
      char c = s.charAt(i);
      switch (c) {
      case '\\':
        buf.append("\\\\");
        break;
      case '"':
        buf.append("\\\"");
        break;
      case '\n':
        buf.append("\\n");
        break;
      case '\r':
        if (lastChar != '\n') {
          buf.append("\\r");
        }
        break;
      default:
        buf.append(c);
        break;
      }
      lastChar = c;
    }
    buf.append('"');
  }

  @Override public boolean equals(@Nullable Object o) {
    // REVIEW: Should constants with the same value and different type
    // (e.g. 3L and 3) be considered equal.
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ConstantExpression that = (ConstantExpression) o;

    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override public int hashCode() {
    return Objects.hash(nodeType, type, value);
  }
}
