import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.TimeZone;
import java.util.Iterator;
import java.util.Collections;
import java.util.List;
import java.sql.Timestamp;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

	//Default mode: replication. Possible string values are "replication" and "sharding"
	private String storageType = "replication";
	private final String r = "replication";		
	private final String s = "sharding";		
	
	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances
	 */
	private static final String dataCenter1 = "ec2-54-164-198-119.compute-1.amazonaws.com";
	private static final String dataCenter2 = "ec2-54-88-215-172.compute-1.amazonaws.com";
	private static final String dataCenter3 = "ec2-54-152-73-223.compute-1.amazonaws.com";


	// key -> (time -> read/write flag)
	// A hashtable that maps the key to a sorted hashtable of timestamps 
	HashMap<String, TreeMap<String, String>> timeQueues = new HashMap<String, TreeMap<String, String>>();
	// key -> read/write flag
	// A hashtable that keeps track of those keys are currently in use
	HashMap<String, String> workingKeys = new HashMap<String, String>();
	// a lock object for synchronization	
	private final Object lock = new Object();

	// add the new request to hashtables
	private void add_to_queue(String key, String time, String flag) {
		synchronized (lock) {
			if (timeQueues.get(key) == null) {
				// initialize the sorted TreeMap if the key is new
				TreeMap t = new TreeMap();

				t.put(time, flag);
				timeQueues.put(key, t);	
			}
			else {
				// insert to the sorted TreeMap
				timeQueues.get(key).put(time, flag);
			}
		}
	}

	// blocks until the key is ready to be processed
	private void wait_until_free(String key, String time, String flag) {
		while (true) {
			synchronized (lock) {
				// make sures that this key is the first in order (time)
				// and no same key is being processed
				if (timeQueues.get(key).firstKey().equals(time) && 
				    workingKeys.get(key) == null) {
					// make sure this key flagged as in use
					workingKeys.put(key, flag);
					return;
				}
				// wait for another thread to wake up 
				try {lock.wait();} catch (Exception e) {}
			}
		}
	}

	// signals other waiting threads that a key just finished processing
	private void signal_done(String key, String time) {
		synchronized (lock) {
			// remove the key from both hashtables
			workingKeys.remove(key);

			timeQueues.get(key).remove(time);
			// wake up waiting threads
			try {lock.notifyAll();} catch (Exception e) {}
		}
	}
	
	// convert a key to the data center number
	private String key_to_dc(String key) {
		if (key.equals("a")) return "1";
		if (key.equals("b")) return "2";
		if (key.equals("c")) return "3";

		int len = key.length();
		int i = 0;
		int sum = 0;
		char[] chars = key.toCharArray();

		// add the ascii value of each character in the string
		while (i < len) {
			sum += ((int)(chars[i]));
			i++;
		}

		// mod the sum by 3 to make sure only 3 possible results
		int j = (sum % 3) + 1;
		if (j == 1) return "1";
		if (j == 2) return "2";
		return "3";
	}

	// update data center(s) with given key and value
	private void update_dc(String key, String value) throws IOException {
		if (storageType.equals(s)) {
			// calculate the data center number for sharding
			String dc = key_to_dc(key);

			//System.out.println("putting (" + key + "," + value + ") to " + dc);

			if (dc.equals("1")) {
				KeyValueLib.PUT(dataCenter1, key, value);
			}
			else if (dc.equals("2")) {
				KeyValueLib.PUT(dataCenter2, key, value);
			}
			else {
				KeyValueLib.PUT(dataCenter3, key, value);
			}
			//System.out.println("done putting (" + key + "," + value + ") to " + dc);
		}

		else {
			// write to all data center for replication
			KeyValueLib.PUT(dataCenter1, key, value);
			KeyValueLib.PUT(dataCenter2, key, value);
			KeyValueLib.PUT(dataCenter3, key, value);
		}
	}

	// get value from data center with given key
	private String fetch_dc(String key, String loc) throws IOException {
		String value = "0";

		if (storageType.equals(s)) {
			// calculate data center number for sharding
			String dc = key_to_dc(key);

			//System.out.println("getting (" + key + ") from " + dc);
			if (loc.equals("1") && loc.equals(dc)) {
				value = KeyValueLib.GET(dataCenter1, key);
			}
			if (loc.equals("2") && loc.equals(dc)) {
				value = KeyValueLib.GET(dataCenter2, key);
			}
			if (loc.equals("3") && loc.equals(dc)) {
				value = KeyValueLib.GET(dataCenter3, key);
			}

			//System.out.println("done getting (" + key + "," + value + ") from " + dc);
		}
		else {
			// randomly pick a data center to get value
			value = KeyValueLib.GET(dataCenter1, key);
		}
		return value;
	}

	@Override
	public void start() {
		//DO NOT MODIFY THIS
		KeyValueLib.dataCenters.put(dataCenter1, 1);
		KeyValueLib.dataCenters.put(dataCenter2, 2);
		KeyValueLib.dataCenters.put(dataCenter3, 3);

		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();

		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");

				//You may use the following timestamp for ordering requests
                final String timestamp = new Timestamp(System.currentTimeMillis() 
                     + TimeZone.getTimeZone("EST").getRawOffset()).toString();

				// add the request info to queue
				add_to_queue(key, timestamp, "write");

				//System.out.println("PUT:" + key + "," + value + "," + timestamp + "," + key_to_dc(key));
				Thread t = new Thread(new Runnable() {
					public void run() {
						// block thread until ready	
						wait_until_free(key, timestamp, "write");

						try{					
							update_dc(key, value);	
						} catch (Exception e) {
							System.out.println("Failed to put " + key + "," + value);
						}

						// signals other waiting threads
						signal_done(key, timestamp);
					}
				});

				t.start();

				//System.out.println("DONE PUT:" + key + "," + value + "," + timestamp);
				req.response().end(); //Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String loc = map.get("loc");

				//You may use the following timestamp for ordering requests
				final String timestamp = new Timestamp(System.currentTimeMillis() 
								+ TimeZone.getTimeZone("EST").getRawOffset()).toString();

				// add the new request info to queue	
				add_to_queue(key, timestamp, "read");

				//System.out.println("GET:" + key + "," + loc + "," + timestamp + "," + key_to_dc(key));
				Thread t = new Thread(new Runnable() {
					public void run() {
						// blocks until ready	
						wait_until_free(key, timestamp, "read");

						String value = "0";

						try{
							value = fetch_dc(key, loc);
						} catch (Exception e) {
							System.out.println("Failed to get " + key);
						}

						// signals other waiting threads
						signal_done(key, timestamp);
						req.response().end(value); //Default response = 0
					}
				});

				t.start();
				//System.out.println("DONE GET:" + key + "," + loc + "," + timestamp);
			}
		});

		routeMatcher.get("/storage", new Handler<HttpServerRequest>() {
                        @Override
                        public void handle(final HttpServerRequest req) {
                                MultiMap map = req.params();
				storageType = map.get("storage");
                                req.response().end();
                        }
                });

		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				
				String response = "Not found.";
				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});

		server.requestHandler(routeMatcher);
		server.listen(8080);
	}
}

