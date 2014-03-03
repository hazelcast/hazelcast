<%@ page
        errorPage="ErrorPage.jsp"
        import="com.hazelcast.wm.test.Value"
        %>
hello1...

<%
    session.setAttribute("value", new Value());
%>
<jsp:forward page="include.jsp"/>
world