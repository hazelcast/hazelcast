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

import static java.util.stream.Collectors.toList;

/**
 * Wraps Calcite's RexNodes, so that connector can be created outside hazelcast-sql module without problems with
 * shaded Calcite dependency.
 *
 * During shading process the {@link RexNode} is shaded into {@code com.hazelcast.shaded.org.apache.calcite.rex.RexNode},
 * however if you work on some {@link SqlConnector} inside core Hazelcast repository, then IDE will still see only
 * class as it is in Maven's jar.
 * <p>
 * Wrapping {@link RexNode}s avoids problems in compilation both in IDE and Maven, as this class is not shaded
 * and in the runtime it will just return shaded class.
 * <strong>If you want to use this class in module outside hazelcast-sql, you must apply maven-shade-plugin with
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
 */
public final class HazelcastRexNode {
    private final RexNode node;

    private HazelcastRexNode(RexNode node) {
        this.node = node;
    }

    /**
     * Returns underlying Calcite {@linkplain RexNode}.
     */
    @Nullable
    public <T> T unwrap(Class<T> clazz) {
        return clazz.cast(node);
    }

    /**
     * Creates a new wrapper around given node.
     */
    public static @Nonnull HazelcastRexNode wrap(RexNode node) {
        return new HazelcastRexNode(node);
    }

    /**
     * Creates a new list with each node from original one wrapped.
     */
    public static @Nonnull List<HazelcastRexNode> wrap(@Nonnull List<RexNode> nodes) {
        return nodes.stream()
                .map(HazelcastRexNode::new)
                .collect(toList());
    }

}
