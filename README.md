# cluster
***
*_Example 1_*

Start ClusterService in default mode. In this mode, the node will be using UDP Broadcast to discover other nodes and will start 
in a next available port of the host machine.

    //Declare the ClusterService
    ClusterService service;
    //Start the service
    service = ClusterService.getInstance(new ClusterConfig());
    //Do your work using ClusterService
    //Stop the service
    service.stop();

*_Example 2_*

Start ClusterService in specified port 9090 and use UDP Multicast group "239.1.2.3." to discover other nodes.


    //Declare the ClusterService 
    ClusterService service;
    //Start the service with port 9090 and Multicast group "239.1.2.3"
    service = ClusterService.getInstance(new ClusterConfig()
                                         .setPort(9090)
                                         .setMulticastGroup("239.1.2.3")
                                        );
    //Do your work using ClusterService
    //Stop the service
    service.stop();

*_Example 3_*

Start ClusterService in specified port 9092 and join to the cluster (created using Example 2 above) using peer node, IP=12.45.23.10 and Port=9090.

    //Declare the ClusterService \r\n
    ClusterService service;
    //Start the service with port 9090 and Multicast group "239.1.2.3"
    service = ClusterService.getInstance(new ClusterConfig()
                                         .setPort(9090)
                                         .setMulticastGroup("239.1.2.3")
                                         .setPeerNode("12.45.23.10:9090")
                                        );
    //Do your work using ClusterService
    //Stop the service
    service.stop();

*_Configuration Settings_*

    ClusterConfig config = new ClusterConfig()
                            .setPort(9090)                    //Set port 9090
                            .setWeight(2)                     //Set node weight 2
                            .setMulticastGroup("239.1.2.3")   //Set multicast group address 239.1.2.3
                            .setMulticastPort(8080)           //Set multicast listen port to 8080
                            .setSocketBacklog(100)            //Set socket incoming request queue size to 100
                            .setHeartBeatInterval(500)        //Set Heart beat interval to 500 ms
                            .setMaxWait(10)                   //Set termination wait time to 10 minutes
                            .setNetworkTimeout(3000)          //Set network timeout to 3000 ms = 3 seconds
                            .setPeerNode("12.34.56.123:9090") //Set peer node to 12.34.56.123:9090
    ;

*_Example 4_*

This will create a Map which shared across all the cluster nodes defined by the ‘service’.
So if ‘node1’ creates a map, that will be immediately available to the ‘node2’, with get() call.

    DistributedMap<String, String> map = new DistributedMap<String, String>(service, "myMap");
    map.put("key1", "value1");
    map.get("key1");

*_Example 5_*

This will submit the task of printing “Hello World!” to the next available node of the cluster of nodes defined by ‘service’.

    DistributedThreadPool pool = new DistributedThreadPool(service, TaskDistributingPolicy.RoundRobin, 20);
    pool.submit(new Runnable() {
                   public void run() {
                                   System.out.println("Hello World!");
                   }
    });

***
# Running the provided binary demo


Make sure you have Java 8 or above runtime environment installed. 
Note: The provided binary is for Windows environment as it packaged windows Swing Jar within it.
Assuming, java command is in your PATH, run following command to launch the demo. Launch, more than one instances to see the working cluster environment.

    java -jar clusterdemo.jar
    
This launches the cluster node in default mode - with UDP broadcast way to discover other nodes.
Following commandline options are available:

    -port <port number>        : Launch the node in a specific port (port must be free and available in the host machine)
    -group <multicast address> : Launch the node in Multicast mode with the multicast address specified as multicast group.
    -peer <host>:<port>        : Launch the node by connecting to a specific peer and discover other nodes through the peer.
    -weight <integer>          : Weightage of the node for weighted load balancing of task.
    
 
