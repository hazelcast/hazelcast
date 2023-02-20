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
package com.hazelcast.jet.sql.impl.connector;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Wraps Calcite's {@link RexNode} so that connector can be created outside hazelcast-sql module without problems with
 * relocated Calcite dependency.
 * <p>
 * During shading process the {@link RexNode} is relocated to {@code com.hazelcast.shaded.*},
 * however if you work on an {@link SqlConnector} inside core Hazelcast repository, then IDE will still see only
 * the class it the original package.
 * <p>
 * Wrapping {@link RexNode}s avoids problems in compilation both in IDE and Maven, as this class is not relocated
 * and at runtime it will just return the relocated class.
 * <strong>If you want to use this class in a module outside hazelcast-sql, you must apply maven-shade-plugin with
 * configuration that shades calcite-core, otherwise you will end up in runtime errors.</strong>
 *
 * <p>
 * To use wrapped {@link RexNode}, call {@link #unwrap(Class)} method, passing {@link RexNode#getClass()} as an argument.
 *
 * <p>
 * Example usage:
 * <pre>{@code
 * HazelcastRexNode hazelcastRexNode = HazelcastRexNode.wrap(someRexNodeEgFilter);
 *
 * // later in SqlConnector, maybe outside hazelcast-sql:
 * RexNode node = hazelcastRexNode.unwrap(RexNode.class);
 * }</pre>
 *
 * Example shade plugin config:
 * <pre>{@code
 *             <plugin>
 *                 <groupId>org.apache.maven.plugins</groupId>
 *                 <artifactId>maven-shade-plugin</artifactId>
 *                 <version>${maven.shade.plugin.version}</version>
 *                 <executions>
 *                     <execution>
 *                         <id>fat-shaded-jar</id>
 *                         <phase>package</phase>
 *                         <goals>
 *                             <goal>shade</goal>
 *                         </goals>
 *                         <configuration>
 *                             <filters>
 *                                 <filter>
 *                                     <artifact>*:*</artifact>
 *                                     <excludes>
 *                                         <exclude>module-info.class</exclude>
 *                                         <exclude>META-INF/*.SF</exclude>
 *                                         <exclude>META-INF/*.DSA</exclude>
 *                                         <exclude>META-INF/*.RSA</exclude>
 *                                     </excludes>
 *                                 </filter>
 *                             </filters>
 *                             <relocations>
 *                                 <relocation>
 *                                     <pattern>org.apache.calcite</pattern>
 *                                     <shadedPattern>${relocation.root}.org.apache.calcite</shadedPattern>
 *                                 </relocation>
 *                             </relocations>
 *                         </configuration>
 *                     </execution>
 *                 </executions>
 *             </plugin>
 * }</pre>
 *
 * Make sure that {@code hazelcast-sql} and {@code org.apache.calcite:calcite-core} are declared
 * with {@code provided} scope, so that they won't be included in final jar.
 */
public final class HazelcastRexNode {
    private final RexNode node;

    private HazelcastRexNode(@Nonnull RexNode node) {
        this.node = requireNonNull(node);
    }

    /**
     * Returns underlying Calcite {@linkplain RexNode}.
     */
    @Nonnull
    public <T> T unwrap(Class<T> clazz) {
        return clazz.cast(node);
    }

    /**
     * Creates a new wrapper around the given `node`.
     */
    public static @Nullable HazelcastRexNode wrap(@Nullable RexNode node) {
        if (node == null) {
            return null;
        }
        return new HazelcastRexNode(node);
    }

    /**
     * Creates a new list with each node from original one wrapped.
     */
    public static @Nonnull List<HazelcastRexNode> wrap(@Nonnull List<RexNode> nodes) {
        return nodes.stream()
                .map(HazelcastRexNode::wrap)
                .collect(toList());
    }
}
