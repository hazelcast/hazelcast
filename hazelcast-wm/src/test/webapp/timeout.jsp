<%
    out.println("session is " + request.getSession(false) + "<p>");
    session.setAttribute("1", "istanbul");
    java.util.Enumeration enumKeys = session.getAttributeNames();
    while (enumKeys.hasMoreElements()) {
        String skey = (String) enumKeys.nextElement();
        String svalue = session.getAttribute(skey).toString();
        out.println(skey + ": " + svalue + " <br>");
    }
    session.setMaxInactiveInterval(3);
%>
<p>

<form action="index.jsp" method="post">
    Key: <input type="text" name="key"/>
    <br/>
    Value:
    <input type="text" name="value"/>
</form>