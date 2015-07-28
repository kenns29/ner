package simpleRestletServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.resource.Directory;
import org.restlet.routing.Router;
import simpleRestletServer.resources.JsonStatus;
import simpleRestletServer.resources.Progress;
import simpleRestletServer.resources.Time;

/**
 * Reusable Reslet Application.  Can be used in different
 * Restlet Components.
 */
public class EmbeddedServerApplication extends Application {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedServerApplication.class);

    /**
     * Constructor.
     */
    public EmbeddedServerApplication() {
        setName("Embedded Reslet HTTP Application");
        setDescription("Short Description.");
        setOwner("Vader Lab");
        setAuthor("Vader Lab");
    }

    /**
     * Creates a Router to dispatch call to server resources.
     * This would be the place to add additional filters.
     */
    @Override
    public Restlet createInboundRoot() {
        String rootUri = "file://"
            + System.getProperty("user.dir")
            + System.getProperty("file.separator")
            + "web"; 
        logger.info("rootUri for file: {}", rootUri);
        Directory directory = new Directory(getContext(), rootUri);
        directory.setListingAllowed(true);

        Router router = new Router(getContext());
        // favicon.ico is now being served automatically from the
        // Directory router below.  However, we leave this here as
        // an example of how to serve an image file from a byte
        // array in memory.
        //router.attach("/favicon.ico", Favicon.class);
        router.attach("/status", simpleRestletServer.resources.Status.class);
        router.attach("/jsonstatus", JsonStatus.class);
        router.attach("/progress", Progress.class);
        router.attach("/time", Time.class);
        // The following will allow all files in the jvm user's default
        // home directory to be read and browsed.
        // It is important that this is the last entry, otherwise it
        // will shadow the above routes.  For example, a request
        // to /status would likely fail because it would look for the
        // status file inside the root directory.
        router.attach("/", directory);
        return router;
    }

}
