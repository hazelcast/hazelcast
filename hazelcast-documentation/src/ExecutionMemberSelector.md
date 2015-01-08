
### Execution Member Selection

As previously mentioned it is possible to indicate where in the Hazelcast cluster the `Runnable` or `Callable` is executed. Usually you will execute in the cluster based on the location of a key, set of keys or just allow Hazelcast to select a member.

There may be times where you want more control over where your code runs. This is where the `MemberSelector` comes into play. For example, you may want certain tasks to run only on certain members, or you may wish to implement some form of custom load balancing regime.  The `MemberSelector` is an interface that you can implement and then provide to the `IExecutorService` when you submit or execute.

The `select(Member)` method is called for every available member in the cluster and it is up to the implementation to decide if the member is going to be used or not.

In a simple example shown below we select members based on the presence of an attribute.

```java
public class MyMemberSelector implements MemberSelector {
     public boolean select(Member member) {
         return Boolean.TRUE.equals(member.getAttribute("my.special.executor"));
     }
 }
```