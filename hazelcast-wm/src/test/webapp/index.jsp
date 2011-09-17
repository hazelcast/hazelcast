<%
        String key = request.getParameter("key");
        if (key != null && !key.equals("")) {
            String value = request.getParameter("value");
            session.setAttribute(key, value);
        }
        java.util.Enumeration enumKeys = session.getAttributeNames();
        while (enumKeys.hasMoreElements()) {
            String skey = (String) enumKeys.nextElement();
            String svalue =  session.getAttribute(skey).toString();
            out.println(skey + ": " + svalue + " <br>");
        }

%>
<p>
<form action="index.jsp" method="post">
Key: <input type="text" name="key" />
<br />
Value:
<input type="text" name="value" />
</form>