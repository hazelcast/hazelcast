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

package cascading.flow;

import java.io.IOException;

import cascading.PlatformTestCase;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import org.junit.Test;

import static data.InputData.inputFileNums10;
import static data.InputData.inputFileNums20;

/**
 *
 */
public class FlowProcessPlatformTest extends PlatformTestCase
  {
  public static class IterateInsert extends BaseOperation implements Function
    {
    private Tap tap;

    public IterateInsert( Fields fieldDeclaration, Tap tap )
      {
      super( fieldDeclaration );
      this.tap = tap;
      }

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall operationCall )
      {
      }

    @Override
    public void cleanup( FlowProcess flowProcess, OperationCall operationCall )
      {
      }

    @Override
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      try
        {
        TupleEntryIterator iterator = flowProcess.openTapForRead( tap );

        while( iterator.hasNext() )
          functionCall.getOutputCollector().add( new Tuple( iterator.next().getTuple() ) );

        iterator.close();
        }
      catch( IOException exception )
        {
        exception.printStackTrace();
        }
      }
    }

  public FlowProcessPlatformTest()
    {
    super( true );
    }

  @Test
  public void testOpenForRead() throws IOException
    {
    getPlatform().copyFromLocal( inputFileNums20 );
    getPlatform().copyFromLocal( inputFileNums10 );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileNums20 );

    Pipe pipe = new Pipe( "test" );

    Tap tap = getPlatform().getTextFile( new Fields( "value" ), inputFileNums10 );

    pipe = new Each( pipe, new IterateInsert( new Fields( "value" ), tap ), Fields.ALL );

    Tap sink = getPlatform().getTextFile( getOutputPath( "openforread" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 200 );
    }
  }
