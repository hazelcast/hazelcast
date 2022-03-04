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

package example.serialization;

import javax.annotation.Nonnull;

public class NodeDTO implements Comparable<NodeDTO> {

    private NodeDTO child;
    private int id;

    public NodeDTO(NodeDTO child, int id) {
        this.child = child;
        this.id = id;
    }

    public NodeDTO(int id) {
        this.id = id;
    }

    public NodeDTO getChild() {
        return child;
    }

    public int getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NodeDTO node = (NodeDTO) o;

        if (id != node.id) {
            return false;
        }
        return child != null ? child.equals(node.child) : node.child == null;
    }

    @Override
    public int hashCode() {
        int result = child != null ? child.hashCode() : 0;
        result = 31 * result + id;
        return result;
    }

    @Override
    public int compareTo(@Nonnull NodeDTO o) {
        return id - o.id;
    }
}
