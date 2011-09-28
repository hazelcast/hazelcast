<%@ page
      errorPage="ErrorPage.jsp"
      import="java.io.*"
      import="java.util.*"
%>
<%= session %>
<%= application	 %>
<%= session.getClass().getName() %>

<form name="input" action="hello.jsp" method="get">
First name: <input type="text" name="key" /><br />
Last name: <input type="text" name="value" />
<input type="submit" value="Submit" />

</form>

<%
   Enumeration enames;
   Map map;
   String title;

   String skey=request.getParameter("key");
   String svalue=request.getParameter("value");
   if (skey != null && svalue != null) {
   	 session.setAttribute(skey, svalue);
   }
   // Print the session attributes

   map = new TreeMap();
   enames = session.getAttributeNames();
   while (enames.hasMoreElements()) {
      String name = (String) enames.nextElement();
      String value = "" + session.getAttribute(name);
      map.put(name, value);
   }
   out.println(createTable(map, "Session Attributes"));

%>

<%-- Define a method to create an HTML table --%>

<%!
   private static String createTable(Map map, String title)
   {
      StringBuffer sb = new StringBuffer();

      // Generate the header lines

      sb.append("<table border='1' cellpadding='3'>");
      sb.append("<tr>");
      sb.append("<th colspan='2'>");
      sb.append(title);
      sb.append("</th>");
      sb.append("</tr>");

      // Generate the table rows

      Iterator imap = map.entrySet().iterator();
      while (imap.hasNext()) {
         Map.Entry entry = (Map.Entry) imap.next();
         String key = (String) entry.getKey();
         String value = (String) entry.getValue();
         sb.append("<tr>");
         sb.append("<td>");
         sb.append(key);
         sb.append("</td>");
         sb.append("<td>");
         sb.append(value);
         sb.append("</td>");
         sb.append("</tr>");
      }

      // Generate the footer lines

      sb.append("</table><p></p>");

      // Return the generated HTML

      return sb.toString();
   }

%>