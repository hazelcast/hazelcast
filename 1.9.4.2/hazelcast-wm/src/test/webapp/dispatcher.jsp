<%
		System.out.println("DISPATCHER " + request);
        RequestDispatcher dispatcher = request.getRequestDispatcher("/hello.jsp");
        dispatcher.forward(request, response);
%>
