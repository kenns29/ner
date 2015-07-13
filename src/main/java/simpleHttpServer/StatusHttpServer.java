package simpleHttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;

import nerAndGeo.Main;

import com.sun.net.httpserver.HttpServer;

public class StatusHttpServer {
	public StatusHttpServer() throws IOException{
		HttpServer server = HttpServer.create(new InetSocketAddress(Main.configPropertyValues.statusHttpServerHost, Main.configPropertyValues.statusHttpServerPort), 0);
        server.createContext("/status", new HttpServerHandler());
        server.createContext("/json", new HttpServerHandlerJSON());
        server.createContext("/test", new HttpServerHandlerTest());
        server.createContext("/progress", new HttpServerHandlerProgress());
        server.setExecutor(null); // creates a default executor
        server.start();
	}
}
