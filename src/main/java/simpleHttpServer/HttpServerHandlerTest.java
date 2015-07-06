package simpleHttpServer;

import java.io.IOException;
import java.io.OutputStream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class HttpServerHandlerTest implements HttpHandler{

	@Override
	public void handle(HttpExchange httpExchange) throws IOException {
		String response = "Hello.";
		httpExchange.sendResponseHeaders(200, response.length());
        OutputStream os = httpExchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
	}

}
