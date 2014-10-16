
### Aggregations Examples

For the final example, imagine you are working for an international company and you have an employee database stored in Hazelcast
`IMap` with all employees worldwide and a `MultiMap` for assigning employees to their certain locations or offices. In addition,
there is another `IMap` which holds the salary per employee.

Let's have a look at our data model:

```java
class Employee implements Serializable {
  private String firstName;
  private String lastName;
  private String companyName;
  private String address;
  private String city;
  private String county;
  private String state;
  private int zip;
  private String phone1;
  private String phone2;
  private String email;
  private String web;

  // getters and setters
}

class SalaryMonth implements Serializable {
  private Month month;
  private int salary;
  
  // getters and setters
}

class SalaryYear implements Serializable {
  private String email;
  private int year;
  private List<SalaryMonth> months;
  
  // getters and setters
  
  public int getAnnualSalary() {
    int sum = 0;
    for ( SalaryMonth salaryMonth : getMonths() ) {
      sum += salaryMonth.getSalary();
    }
    return sum;
  }
}
```

The two `IMap`s and the `MultiMap`, they are both keyed by the string of email and are defined as follows:

```java
IMap<String, Employee> employees = hz.getMap( "employees" );
IMap<String, SalaryYear> salaries = hz.getMap( "salaries" );
MultiMap<String, String> officeAssignment = hz.getMultiMap( "office-employee" );
```

So far, we know all important information to work out some example aggregations. We will look into some deeper implementation
details and how we can work around some current limitations that will be eliminated in future versions of the API.

So let's start with an already seen, very basic example. We want to know the average salary of all of our employees. To do this,
we need a `PropertyExtractor` and the average aggregation for type `Integer`.

```java
IMap<String, SalaryYear> salaries = hazelcastInstance.getMap( "salaries" );
PropertyExtractor<SalaryYear, Integer> extractor =
    (salaryYear) -> salaryYear.getAnnualSalary();
int avgSalary = salaries.aggregate( Supplier.all( extractor ),
                                    Aggregations.integerAvg() );
```

That's it. Internally, we created a map-reduce task based on the predefined aggregation and fire it up immediately. Currently, all
aggregation calls are blocking operations, so it is not yet possible to execute the aggregation in a reactive way (using
`com.hazelcast.core.ICompletableFuture`) but this will be part of one of the upcoming versions.

#### Map Join Example

The following example is already a bit more complex, so we only want to have our US based employees selected into the average
salary calculation, so we need to execute some kind of a join operation between the employees and salaries maps.

```java
class USEmployeeFilter implements KeyPredicate<String>, HazelcastInstanceAware {
  private transient HazelcastInstance hazelcastInstance;
  
  public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
    this.hazelcastInstance = hazelcastInstance;
  }
  
  public boolean evaluate( String email ) {
    IMap<String, Employee> employees = hazelcastInstance.getMap( "employees" );
    Employee employee = employees.get( email );
    return "US".equals( employee.getCountry() );
  }
}
```

Using the `HazelcastInstanceAware` interface, we get the current instance of Hazelcast injected into our filter and can perform data
joins on other data structures of the cluster. We now only select employees that work as part of our US offices into the
aggregation.

```java
IMap<String, SalaryYear> salaries = hazelcastInstance.getMap( "salaries" );
PropertyExtractor<SalaryYear, Integer> extractor =
    (salaryYear) -> salaryYear.getAnnualSalary();
int avgSalary = salaries.aggregate( Supplier.fromKeyPredicate(
                                        new USEmployeeFilter(), extractor
                                    ), Aggregations.integerAvg() );
```

#### Grouping Example

For our next example, we will do some grouping based on the different worldwide offices. Currently, a group aggregator is not yet 
available, that means we need a small workaround to achieve this goal. In later versions of the Aggregations API this will not be 
required anymore since it will be available out of the box in a much more convenient way.

So again, let's start with our filter. This time we want to filter based on an office name and we again need to do some data joins
to achieve this kind of filtering. 

**A short tip:** to minimize the data transmission on the aggregation we can use
[Data Affinity](#data-affinity) rules to influence the partitioning of data. Be aware that this is an expert feature of Hazelcast.

```java
class OfficeEmployeeFilter implements KeyPredicate<String>, HazelcastInstanceAware {
  private transient HazelcastInstance hazelcastInstance;
  private String office;
  
  // Deserialization Constructor
  public OfficeEmployeeFilter() {
  } 
  
  public OfficeEmployeeFilter( String office ) {
    this.office = office;
  }
  
  public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
    this.hazelcastInstance = hazelcastInstance;
  }
  
  public boolean evaluate( String email ) {
    MultiMap<String, String> officeAssignment = hazelcastInstance
        .getMultiMap( "office-employee" );

    return officeAssignment.containsEntry( office, email );    
  }
}
```

Now, we can execute our aggregations. As mentioned, we currently need to do the grouping on our own by executing multiple
aggregations in a row but that will go away soon.

```java
Map<String, Integer> avgSalariesPerOffice = new HashMap<String, Integer>();

IMap<String, SalaryYear> salaries = hazelcastInstance.getMap( "salaries" );
MultiMap<String, String> officeAssignment =
    hazelcastInstance.getMultiMap( "office-employee" );

PropertyExtractor<SalaryYear, Integer> extractor =
    (salaryYear) -> salaryYear.getAnnualSalary();

for ( String office : officeAssignment.keySet() ) {
  OfficeEmployeeFilter filter = new OfficeEmployeeFilter( office );
  int avgSalary = salaries.aggregate( Supplier.fromKeyPredicate( filter, extractor ),
                                      Aggregations.integerAvg() );
                                      
  avgSalariesPerOffice.put( office, avgSalary );
}
```

#### Simple Count Example

After the previous example, we want to fade out from this section by executing one final, easy but nice aggregation. We
just want to know how many employees we currently have on a worldwide basis. Before reading the next lines of source code, you
can try to do it on your own to see if you understood the way of executing aggregations.

As said, this is again a very basic example but it is the perfect closing point for this section:

```java
IMap<String, Employee> employees = hazelcastInstance.getMap( "employees" );
int count = employees.size();
```

Ok, after that quick joke, we look at the real two code lines:

```java
IMap<String, Employee> employees = hazelcastInstance.getMap( "employees" );
int count = employees.aggregate( Supplier.all(), Aggregations.count() );
```

We now have a good overview of how to use aggregations in real life situations. If you want to do your colleagues a favor you
might want to end up writing your own additional set of aggregations. Then please read on the next section, if not just stop
here.

