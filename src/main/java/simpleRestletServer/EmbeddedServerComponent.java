package simpleRestletServer;

import org.restlet.Component;
import org.restlet.Context;
import org.restlet.Server;
import org.restlet.data.Protocol;
import org.restlet.routing.VirtualHost;

public class EmbeddedServerComponent extends Component{
	/**
     * Constructor.
     * 
     * @throws Exception
     */
    public EmbeddedServerComponent(int port) throws Exception {
        // Set basic properties
        setName("Embedded Reslet HTTP Component");
        setDescription("Short Description.");
        setOwner("Vader Lab");
        setAuthor("Vader Lab");

        // Add connectors
        Server server = new Server(new Context(), Protocol.HTTP, port);
        server.getContext().getParameters().set("tracing", "true");
        getServers().add(server);
        getClients().add(Protocol.FILE); 

        // Configure the default virtual host
        VirtualHost host = getDefaultHost();
        // host.setHostDomain("www\\.rmep\\.com|www\\.rmep\\.net|www\\.rmep\\.org");
        // host.setServerAddress("1\\.2\\.3\\.10|1\\.2\\.3\\.20");
        // host.setServerPort("80");

        // Attach the application to the default virtual host
        host.attachDefault(new EmbeddedServerApplication());
    }
}
