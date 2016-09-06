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

package cascading.flow.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import cascading.PlatformTestCase;
import cascading.flow.stream.util.SparseTupleComparator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

/**
 *
 */
public class SparseTupleFieldsComparatorTest extends PlatformTestCase
  {
  public SparseTupleFieldsComparatorTest()
    {
    }

  @Test
  public void testCompare()
    {
    List<Tuple> result = new ArrayList<Tuple>();

    result.add( new Tuple( "1", "1", "1" ) );
    result.add( new Tuple( "2", "10", "1" ) );
    result.add( new Tuple( "3", "1", "1" ) );

    runTest( new Fields( "a", "b" ), result, null );

    result = new ArrayList<Tuple>();

    result.add( new Tuple( "1", "1", "1" ) );
    result.add( new Tuple( "3", "1", "1" ) );
    result.add( new Tuple( "2", "10", "1" ) );

    runTest( new Fields( "b", "a" ), result, null ); // sort field b first

    result = new ArrayList<Tuple>();

    result.add( new Tuple( "2", "10", "1" ) );
    result.add( new Tuple( "1", "1", "1" ) );
    result.add( new Tuple( "3", "1", "1" ) );

    runTest( new Fields( "c" ), result, getPlatform().getStringComparator( true ) );
    }

  private void runTest( Fields sortFields, List<Tuple> result, Comparator defaultComparator )
    {
    Fields fields = new Fields( "a", "b", "c" );

    List<Tuple> list = new ArrayList<Tuple>();

    list.add( new Tuple( "2", "10", "1" ) );
    list.add( new Tuple( "1", "1", "1" ) );
    list.add( new Tuple( "3", "1", "1" ) );

    Collections.sort( list, new SparseTupleComparator( fields, sortFields, defaultComparator ) );

    assertEquals( result, list );
    }
  }

