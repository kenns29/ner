package simpleHttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;

import nerAndGeo.Main;

import com.sun.net.httpserver.HttpServer;

public class StatusHttpServer {
	public StatusHttpServer() throws IOException{
		HttpServer server = HttpServer.create(new InetSocketAddress(Main.configPropertyValues.statusHttpServerHost, Main.configPropertyValues.statusHttpServerPort), 0);
        server.createContext("/" + Main.configPropertyValues.statusHttpServerPath, new HttpServerHandler());
        server.createContext("/test", new HttpServerHandlerTest());
        server.setExecutor(null); // creates a default executor
        server.start();
	}
}
