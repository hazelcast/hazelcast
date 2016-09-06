/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading;

import java.util.Iterator;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.junit.Test;

import static data.InputData.inputFileLhs;

public class UnmodifiablePipesPlatformTest extends PlatformTestCase
  {
  public UnmodifiablePipesPlatformTest()
    {
    super( false ); // no need for clustering
    }

  public static class TestFunction extends BaseOperation implements Function
    {
    public TestFunction()
      {
      super( Fields.ARGS );
      }

    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      if( !functionCall.getArguments().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      if( !functionCall.getArguments().getTuple().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      Tuple result = new Tuple( functionCall.getArguments().getTuple() );

      functionCall.getOutputCollector().add( result );

      if( result.isUnmodifiable() )
        throw new IllegalStateException( "is unmodifiable" );
      }
    }

  public static class TestFilter extends BaseOperation implements Filter
    {
    public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
      {
      if( !filterCall.getArguments().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      if( !filterCall.getArguments().getTuple().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      return false;
      }
    }

  public static class TestAggregator extends BaseOperation implements Aggregator
    {
    public TestAggregator()
      {
      super( Fields.ARGS );
      }

    public void start( FlowProcess flowProcess, AggregatorCall aggregatorCall )
      {
      if( !aggregatorCall.getGroup().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      if( !aggregatorCall.getGroup().getTuple().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );
      }

    public void aggregate( FlowProcess flowProcess, AggregatorCall aggregatorCall )
      {
      if( !aggregatorCall.getGroup().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      if( !aggregatorCall.getGroup().getTuple().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      if( !aggregatorCall.getArguments().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      if( !aggregatorCall.getArguments().getTuple().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );
      }

    public void complete( FlowProcess flowProcess, AggregatorCall aggregatorCall )
      {
      if( !aggregatorCall.getGroup().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      if( !aggregatorCall.getGroup().getTuple().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      Tuple result = new Tuple( "some value" );

      aggregatorCall.getOutputCollector().add( result );

      if( result.isUnmodifiable() )
        throw new IllegalStateException( "is unmodifiable" );
      }
    }

  public static class TestBuffer extends BaseOperation implements Buffer
    {
    public TestBuffer()
      {
      super( Fields.ARGS );
      }

    public void operate( FlowProcess flowProcess, BufferCall bufferCall )
      {
      if( !bufferCall.getGroup().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      if( !bufferCall.getGroup().getTuple().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      if( bufferCall.getJoinerClosure() != null )
        throw new IllegalStateException( "joiner closure should be null" );

      Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();

      while( iterator.hasNext() )
        {
        TupleEntry tupleEntry = iterator.next();

        if( !tupleEntry.isUnmodifiable() )
          throw new IllegalStateException( "is modifiable" );

        if( !tupleEntry.getTuple().isUnmodifiable() )
          throw new IllegalStateException( "is modifiable" );

        Tuple result = new Tuple( tupleEntry.getTuple() );

        bufferCall.getOutputCollector().add( result );

        if( result.isUnmodifiable() )
          throw new IllegalStateException( "is unmodifiable" );
        }
      }
    }

  @Test
  public void testUnmodifiable() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "lhs", "rhs" ), " ", inputFileLhs );
    Tap sink = getPlatform().getTextFile( getOutputPath( "simple" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "lhs" ), new TestFunction(), Fields.REPLACE );
    pipe = new Each( pipe, new Fields( "lhs" ), new TestFilter() );

    pipe = new GroupBy( pipe, new Fields( "lhs" ) );

    pipe = new Every( pipe, new Fields( "rhs" ), new TestAggregator(), Fields.ALL );
    pipe = new Each( pipe, new Fields( "lhs" ), new TestFunction(), Fields.REPLACE );

    pipe = new GroupBy( pipe, new Fields( "lhs" ) );
    pipe = new Every( pipe, new Fields( "lhs" ), new TestBuffer(), Fields.RESULTS );
    pipe = new Each( pipe, new Fields( "lhs" ), new TestFunction(), Fields.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, null );
    }
  }