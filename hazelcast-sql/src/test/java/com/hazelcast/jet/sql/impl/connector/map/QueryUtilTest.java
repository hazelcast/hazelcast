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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;

import javax.security.auth.Subject;
import java.io.IOException;
import java.security.Principal;
import java.util.LinkedList;

import static com.hazelcast.jet.core.JetTestSupport.TEST_SS;
import static com.hazelcast.jet.sql.impl.connector.map.QueryUtil.toPredicate;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryUtilTest {

    @Test
    public void when_leftValueIsNull_then_returnsNull() {
        Predicate<Object, Object> predicate =
                toPredicate(new JetSqlRow(TEST_SS, new Object[1]), new int[]{0}, new int[]{0}, new QueryPath[]{QueryPath.KEY_PATH});

        assertThat(predicate).isNull();
    }

    @Test
    public void writeAndReadJoinProjection() throws IOException {
        final var evalContextWithSubjectMock = mock(ExpressionEvalContext.class);
        when(evalContextWithSubjectMock.getSerializationService()).thenReturn(mock());
        when(evalContextWithSubjectMock.getArguments()).thenReturn(singletonList("value"));
        final var subject = new Subject(true, singleton(mock(Principal.class)), emptySet(), emptySet());
        when(evalContextWithSubjectMock.subject()).thenReturn(subject);

        final var evalContextNoSubjectMock = mock(ExpressionEvalContext.class);
        when(evalContextNoSubjectMock.getSerializationService()).thenReturn(mock());

        final var queue = new LinkedList<>();
        final var output = mock(ObjectDataOutput.class, (Answer<Void>) invocation -> {
            queue.add(invocation.getArguments()[0]);
            return null;
        });
        final var input = mock(ObjectDataInput.class, (Answer<Object>) invocation -> queue.poll());

        DataSerializable writer = (DataSerializable) QueryUtil.toProjection(mock(), evalContextWithSubjectMock);
        DataSerializable reader = (DataSerializable) QueryUtil.toProjection(mock(), evalContextNoSubjectMock);

        writer.writeData(output);
        reader.readData(input);

        assertThat(reader)
                .usingRecursiveComparison()
                .comparingOnlyFields("rowProjectorSupplier", "arguments", "subject")
                .ignoringFields("subject.pubCredentials", "subject.privCredentials")
                .isEqualTo(writer);
    }

    @Test
    public void readDataFailedIfUnexpectedObjectInInput() {
        final var evalContextMock = mock(ExpressionEvalContext.class);
        when(evalContextMock.getSerializationService()).thenReturn(mock());

        final var input = mock(ObjectDataInput.class, (Answer<Object>) invocation -> "some unexpected object");

        DataSerializable reader = (DataSerializable) QueryUtil.toProjection(mock(), evalContextMock);
        assertThatThrownBy(() -> reader.readData(input)).isInstanceOf(ClassCastException.class);
    }
}
