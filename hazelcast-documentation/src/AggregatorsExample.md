
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

The two `IMap`s and the `MultiMap` are keyed by the string of email. They are defined as follows:

```java
IMap<String, Employee> employees = hz.getMap( "employees" );
IMap<String, SalaryYear> salaries = hz.getMap( "salaries" );
MultiMap<String, String> officeAssignment = hz.getMultiMap( "office-employee" );
```

So far, we know all the important information to work out some example aggregations. We will look into some deeper implementation
details and how we can work around some current limitations that will be eliminated in future versions of the API.

Let's start with a very basic example. We want to know the average salary of all of our employees. To do this,
we need a `PropertyExtractor` and the average aggregation for type `Integer`.

```java
IMap<String, SalaryYear> salaries = hazelcastInstance.getMap( "salaries" );
PropertyExtractor<SalaryYear, Integer> extractor =
    (salaryYear) -> salaryYear.getAnnualSalary();
int avgSalary = salaries.aggregate( Supplier.all( extractor ),
                                    Aggregations.integerAvg() );
```

That's it. Internally, we created a MapReduce task based on the predefined aggregation and fired it up immediately. Currently, all
aggregation calls are blocking operations, so it is not yet possible to execute the aggregation in a reactive way (using
`com.hazelcast.core.ICompletableFuture`) but this will be part of an upcoming version.

#### Map Join Example

The following example is a little more complex. We only want to have our US based employees selected into the average
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

Using the `HazelcastInstanceAware` interface, we get the current instance of Hazelcast injected into our filter and we can perform data
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
available, so we need a small workaround to achieve this goal. (In later versions of the Aggregations API this will not be 
required because it will be available out of the box in a much more convenient way.)

Again, let's start with our filter. This time, we want to filter based on an office name and we need to do some data joins
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

Now we can execute our aggregations. As mentioned before, we currently need to do the grouping on our own by executing multiple
aggregations in a row.

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

After the previous example, we want to end this section by executing one final and easy aggregation. We
want to know how many employees we currently have on a worldwide basis. Before reading the next lines of example code, you
can try to do it on your own to see if you understood how to execute aggregations.

```java
IMap<String, Employee> employees = hazelcastInstance.getMap( "employees" );
int count = employees.size();
```

Ok, after that quick joke, we look at the real two code lines:

```java
IMap<String, Employee> employees = hazelcastInstance.getMap( "employees" );
int count = employees.aggregate( Supplier.all(), Aggregations.count() );
```

We now have an overview of how to use aggregations in real life situations. If you want to do your colleagues a favor, you
might want to write your own additional set of aggregations. If so, then read the next section, [Implementing Aggregations](#implementing-aggregations).

