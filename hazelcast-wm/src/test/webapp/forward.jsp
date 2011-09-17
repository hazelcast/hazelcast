<%@ page
      errorPage="ErrorPage.jsp"
      import="java.io.*"
      import="java.util.*"
      import="com.hazelcast.wm.test.*"
%>
hello1...

<%
  System.out.println ("CREATING SESSION value " + new Value());
  session.setAttribute("value", new Value());
%>
<jsp:forward page="hello.jsp" /> 
world