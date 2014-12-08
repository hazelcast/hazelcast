## Member Attributes Configuration

It is used to tag your cluster members in case, for example, you want to direct some tasks to specific members.

**Declarative Configuration:**

```xml
<member-attributes name="twentyFourCore">
   <attribute name="CPU_CORE_COUNT" type="int">24</attribute>
</member-attributes>
```

**Programmatic Configuration:**

```java
MemberAttributeConfig twentyFourCore = new MemberAttributeConfig(); 
memberAttributeConfig.setIntAttribute( "CPU_CORE_COUNT", 24 );
Config member1Config = new Config();
config.setMemberAttributeConfig( twentyFourCore );
HazelcastInstance member1 = Hazelcast.newHazelcastInstance( member1Config );
```


It has only "attribute" parameter.

- attribute: You specify the name, type and value of your attribute here.





