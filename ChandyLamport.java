
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.Random;


public class ChandyLamport implements Runnable {

	String type;
	String port;
	String hostname;
	String remotehostname;
	String accessType;
	BufferedReader readr;
	PrintWriter writr;

	static String NodeId;
	static String state = "active";
	static int vectorClock[];
	static int minPerActive;
	static int maxPerActive;
	static int randMsgs;
	static int minSendDelay;
	static int num_connections;
	static int snapShotDelay;
	static int maxNumber = 0;
	static int totalAppMsgSent = 0;
	static String parentNode;

	static String configOutFile = "";
	static String randNeighbor;
	static String prevTimeStamp = "";

	static boolean snapshot_started = false;

	static ArrayList<String> nodeNeighbors;
	static ArrayList<String> bufferAppMsgs = new ArrayList<>();
	static ArrayList<String> receivedMsgs = new ArrayList<>();
	static ArrayList<String> childNodes = new ArrayList<>();
	static HashMap<String, Boolean> receive_marker = new HashMap<String, Boolean>();
	static HashMap<String, Boolean> send_marker = new HashMap<String, Boolean>();
	static HashMap<String, String> nodeMachineMap = new HashMap<String, String>();
	static HashMap<String, String> nodePortMap = new HashMap<String, String>();
	static HashMap<String, String> machinetoNodeMap;
	static HashMap<String, Boolean> childInfoMap = new HashMap<String,Boolean>();
	
	/* Store the input stream of all nodes in readerMap */
	static HashMap<String, BufferedReader> readerMap = new HashMap<>();
	/* Store the output stream of all nodes in writerMap */
	static HashMap<String, PrintWriter> writerMap = new HashMap<>();

	public ChandyLamport(String type, String port, String hostname) {
		this.type = type;
		this.port = port;
		this.hostname = hostname;
	}

	public ChandyLamport(BufferedReader readr, String remotehostname, String accessType) {
		this.readr = readr;
		this.remotehostname = remotehostname;
		this.accessType = accessType;
	}

	public ChandyLamport(String accessType) {

		this.accessType = accessType;
	}

	public synchronized void incrementVectorClock(int[] recvVectorClock) {

		// System.out.println("received vector " +
		// Arrays.toString(recvVectorClock));
		// System.out.println("current vector clock " +
		// Arrays.toString(vectorClock));

		vectorClock[Integer.parseInt(NodeId)]++;

		for (int i = 0; i < vectorClock.length; i++) {
			if (vectorClock[i] < recvVectorClock[i]) {
				vectorClock[i] = recvVectorClock[i];
			}
		}

		// System.out.println("updated vector clock " +
		// Arrays.toString(vectorClock));
	}

	public synchronized void addToBuffer(String vector) {
		if (!bufferAppMsgs.contains(vector))
			bufferAppMsgs.add(vector);
	}

	public synchronized void changeState(String s) {
		state = s;
	}

	public synchronized void startSnapShot() {
		snapshot_started = true;
	}

	public synchronized void incrementTotalSentMsgs() {
		totalAppMsgSent++;
	}

	public synchronized void sendMarkerMessages() {

		for (int i = 0; i < nodeNeighbors.size(); i++) {
			PrintWriter writer = writerMap.get(nodeMachineMap.get(nodeNeighbors.get(i)));
			writer.println("Marker Message ");
		}
		// System.out.println("Sent all marker messages");

	}

	public static void createConfigOutFile() throws IOException {

		configOutFile = "config_name" + "-" + NodeId + ".out";
		File cout = new File(configOutFile);
		cout.createNewFile();

	}

	public synchronized void writeToConfigOutFile() throws IOException {

		String vectorTimeStamp = "";
		for (int i = 0; i < vectorClock.length; i++) {
			vectorTimeStamp += vectorClock[i] + " ";
		}
		// System.out.println(vectorTimeStamp);

		// System.out.println("current timestamp "+vectorTimeStamp);
		// System.out.println("prevTimeStamp "+prevTimeStamp);

		if (prevTimeStamp.equals(vectorTimeStamp)) {
			// System.out.println("sametimestamp do not write");
			return;
		}

		prevTimeStamp = vectorTimeStamp;
		FileWriter writer = new FileWriter(configOutFile, true);
		writer.write(vectorTimeStamp + "\n");
		writer.flush();
		writer.close();

	}

	public synchronized void writeApplicationMessages() {

		if (totalAppMsgSent <= maxNumber) {

			if (state.equals("active") && snapshot_started == false) {

				Random rand = new Random();
				randMsgs = rand.nextInt((maxPerActive - minPerActive) + 1) + minPerActive;
				// System.out.println("Send application messages");

				for (int i = 0; i < randMsgs; i++) {

					Random r = new Random();
					randNeighbor = nodeNeighbors.get(r.nextInt(nodeNeighbors.size()));
					PrintWriter writer = writerMap.get(nodeMachineMap.get(randNeighbor));
					String sendMsg = "Application Message  ";
					for (int j = 0; j < vectorClock.length; j++) {
						sendMsg += vectorClock[j] + " ";
					}

					// System.out.println("incrementing vector clock write
					// msgs");
					// System.out.println("totalAppMsgSent " + totalAppMsgSent +
					// " maxNumber " + maxNumber);
					// System.out.println("randMsgs "+randMsgs);
					incrementVectorClock(vectorClock);
					writer.println(sendMsg);
					incrementTotalSentMsgs();
					if (totalAppMsgSent > maxNumber) {
						changeState("passive");
						break;
					}

					try {
						Thread.sleep(minSendDelay);
					} catch (InterruptedException e) {
					    //e.printStackTrace();
					}
				}
//				System.out.println("Sent " + randMsgs + " number of application messages");
				changeState("passive");

//				System.out.println("totalSentMessages " + totalAppMsgSent + " state " + state);

			}
		}

	}

	public synchronized void stopSnapShot() {
		snapshot_started = false;
	}

	public synchronized boolean checkAllMarkersReceived() {

		for (int i = 0; i < nodeNeighbors.size(); i++) {

			if (receive_marker.get(nodeNeighbors.get(i)) == false) {
				return false;
			}

		}

		for (String key : receive_marker.keySet()) {
			receive_marker.put(key, false);
		}

		stopSnapShot();

		return true;
	}

	public synchronized void updateReceiveMarkerMap(String host) {
		receive_marker.put(machinetoNodeMap.get(host), true);
	}

	public synchronized void updateInTransitMessages() {

		ArrayList<String> tempAppMsgs = bufferAppMsgs;
		for (int i = 0; i < tempAppMsgs.size(); i++) {
			String[] recvVectorClockTemp = bufferAppMsgs.get(i).split(" ");

			int[] recvVectorClock = new int[recvVectorClockTemp.length];

			for (int j = 0; j < recvVectorClock.length; j++) {
				recvVectorClock[j] = Integer.parseInt(recvVectorClockTemp[j]);
			}

			incrementVectorClock(recvVectorClock);
			bufferAppMsgs.remove(i);
		}

	}

	public static boolean isServerListening(String host, int port) {
		Socket s = null;
		try {
			s = new Socket(host, port);
			return true;

		} catch (Exception e) {
			return false;
		} finally {

			if (s != null)
				try {
					s.close();
				} catch (Exception e) {

				}
		}
	}

	public static synchronized void incrConnections() {
		num_connections++;
	}

	public synchronized boolean detectTermination() throws InterruptedException {

		boolean flag = false;

		for (int i = 0; i < 5; i++) {
			flag = false;
			if (state.equals("passive") && bufferAppMsgs.size() == 0) {
				flag = true;
				Thread.sleep(1000);
			}
		}

		return flag;

	}

	public synchronized void sendStateToParent() {

		PrintWriter writer = writerMap.get(nodeMachineMap.get(parentNode));
		String sendMsg = "State is Passive and all channels are empty";
		writer.println(sendMsg);

	}
	
	public synchronized void receivedInfoFromChild(String remHostName) {
		
		String childId = machinetoNodeMap.get(remHostName);
		childInfoMap.put(childId, true);
		
	}
	
	public synchronized boolean receivedInfoFromAllChildren() {
		
//	    System.out.println("ChildInfoMap "+childInfoMap.toString());
		for(String key: childInfoMap.keySet()) {
			if(childInfoMap.get(key) == false) {
				return false;
			}
		}
		
		return true;
	}
	
	public synchronized void sendFinishMessage() {
		
		if(childNodes == null) 
			return;
			
		for(int i=0; i< childNodes.size(); i++) {
		
			PrintWriter writer = writerMap.get(nodeMachineMap.get(childNodes.get(i)));
			writer.println("Finish Message");
		}
		
	}
	
	public void run() {

		if (type == "server") {

			try {
				// Create a server socket at port 5000
//				System.out.println("Starting server socket on port " + port);
				ServerSocket serverSock = new ServerSocket(Integer.parseInt(port));
				// Server goes into a permanent loop accepting connections from
				// clients

				while (true) {
					// Listens for a connection to be made to this socket and
					// accepts it
					// The method blocks until a connection is made
					Socket sock = serverSock.accept();

					String remoteHostName = sock.getInetAddress().getHostName();

					remoteHostName = remoteHostName.substring(0, 4);
					// PrintWriter is a bridge between character data and the
					// socket's low-level output stream
					PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);
					BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));

					if (!readerMap.containsKey(remoteHostName))
						incrConnections();
					readerMap.put(remoteHostName, reader);
					writerMap.put(remoteHostName, writer);

					// System.out.println("numconnections " + num_connections +
					// " remotehostname " + remoteHostName);
				}

			} catch (IOException ex) {
				// ex.printStackTrace();
			}

		} else if (type == "client") {

			/* Wait for the server to be listening */
			while (true) {

				if (isServerListening(hostname, Integer.parseInt(port))) {
					break;
				}

			}

			try {
				Socket clientSocket = new Socket(hostname, Integer.parseInt(port));
				BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
				PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
//				System.out.println("Connected to server " + hostname);
				if (!readerMap.containsKey(hostname))
					incrConnections();
				readerMap.put(hostname, reader);
				writerMap.put(hostname, writer);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}

		} else {
			/*
			 * Check the input streams of each client and if there is any new
			 * message, update the respective counters
			 */

			if (accessType == "read") {

				String received_msg = "";
				try {
					while ((received_msg = readr.readLine()) != null) {

						String recv_msg_type = received_msg.split(" ")[0];

						// System.out.println("recv_msg_type " +
						// recv_msg_type);
						// System.out.println("received_msg " + received_msg);
						// System.out.println("snapshot_state " +
						// snapshot_started);
						if (recv_msg_type.equals("Application")) {

//							System.out.println("received " + received_msg + " from neighbor machine " + remotehostname);
							/*
							 * if(receivedMsgs.contains(received_msg)) continue;
							 * receivedMsgs.add(received_msg);
							 */
							/*
							 * Increment the vector clock of this process if no
							 * marker has been received yet
							 */
							if (totalAppMsgSent < maxNumber)
								changeState("active");
							String receivedVector = received_msg.split("  ")[1];
							String[] recvVectorClockTemp = receivedVector.split(" ");
							int[] recvVectorClock = new int[recvVectorClockTemp.length];
							if (snapshot_started == false) {

								for (int i = 0; i < recvVectorClock.length; i++) {
									recvVectorClock[i] = Integer.parseInt(recvVectorClockTemp[i]);
								}
								// System.out.println("incrementing vector clock
								// received msgs");
								incrementVectorClock(recvVectorClock);

							} else {

								/*
								 * Wait till all markers are received from your
								 * neighbors
								 */
								addToBuffer(receivedVector);

							}
						} else if (recv_msg_type.equals("Marker")) {

							updateReceiveMarkerMap(remotehostname);

							/*
							 * If all markers are received, check if there any
							 * intransit messages, and update the vector clock
							 * accordingly
							 */
							if (checkAllMarkersReceived()) {

								/*
								 * Retrieve the buffered messages, update the
								 * vector clocks accordingly
								 */
								updateInTransitMessages();

							}

							/*
							 * If the snapshot is not started yet, start the
							 * snapshot
							 */
							if (snapshot_started == false) {

								/*
								 * update the variable snapshot_started to true
								 */
								Thread.sleep(snapShotDelay);
								startSnapShot();

								/*
								 * update the vector clock value to the
								 * config-out file
								 */
								//System.out.println("Taking snapshot");
								writeToConfigOutFile();
								/*
								 * send marker messages along all neighbors
								 */
								sendMarkerMessages();

							}
						} else if (recv_msg_type.equals("Finish")) {
							sendFinishMessage();
							Thread.sleep(5000); 
							//Wait for child to get the message
							System.out.println("Received Finish Message.. Exiting..");
							System.exit(0);	
							
						} else if(recv_msg_type.equals("State")) {
							receivedInfoFromChild(remotehostname);
						}
						
					}
					

				} catch (NumberFormatException | IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					// e.printStackTrace();
				}
			} else if (accessType == "write") {

				/*
				 * If you are node 0, then send the first application message to
				 * randomly selected neighbor
				 */

				/*
				 * Select a random number in between minPerActive and
				 * maxPerActive - use a rand() function for this
				 */
				while (true) {
					writeApplicationMessages();

					/*
					 * If all application messages are sent and if the state is
					 * passive and if all channels are empty, chandylamport
					 * snapshot is complete
					 */
					// System.out.println("buffer msgs "+bufferAppMsgs);

				}

			} else if (accessType == "snapshot") {

				/* Node 0 is the one starting the snapshot */

				startSnapShot();
				sendMarkerMessages();

			} else {

				try {
					while (detectTermination() == false) {

						Thread.sleep(1);
						/*
						 * If state is passive and all channels are empty, send
						 * this info to parent node
						 */

					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
				}
				
				while(true) {
				
				    if(NodeId.equals("0") && receivedInfoFromAllChildren()) {
					sendFinishMessage();
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("sending finish message ");
					 System.exit(0);
				    } else if(receivedInfoFromAllChildren()) {
					sendStateToParent();
//					System.out.println("received info from all children");
					break;
				    }
				}
				//  System.out.println("Send info to parent");
			}
		}
	}

	public static void main(String args[]) throws IOException, InterruptedException {

		/* Get the data from the readConfigFile */
		readConfigFile rc = new readConfigFile();
		
		rc.invoke();
		/* Pass my hostname and get my nodeId */
		NodeId = rc.getNodeId();

		/* Create a config-out file with this NodeId */
		createConfigOutFile();

		if (NodeId.equals("0")) {
			state = "active";
		}

		int totalNodes = rc.getTotalNodes();
		nodeNeighbors = rc.getNeighbors(NodeId);
		nodeMachineMap = rc.getNodeMachineMap();
		nodePortMap = rc.getNodePortMap();
		vectorClock = new int[totalNodes];
		machinetoNodeMap = rc.machineToNodeMap;
		maxNumber = rc.maxNumber;
		
		
		sptree spt = new sptree();
		spt.invoke();
		//        System.out.println("Spanning Tree : "  +  spt.TreeMap);                                                                                       
        
		spt.ParentMap("0");
		// System.out.println("parent of 4 "+spt.getParent(NodeId));
		// System.out.println("children of 0 "+spt.getChild("0"));
		childNodes = spt.getChild(NodeId);

		parentNode = spt.getParent(NodeId);

		for (int i = 0; i < nodeNeighbors.size(); i++) {
			receive_marker.put(nodeNeighbors.get(i), false);
		}

		String[] numOfMsgs = rc.numOfMsgs();
		minPerActive = Integer.parseInt(numOfMsgs[0]);
		maxPerActive = Integer.parseInt(numOfMsgs[1]);
		minSendDelay = rc.minSendDelay;
		snapShotDelay = rc.snapShotDelay;

		/*
		 * Else check if the neighbor list of this neighbor has you as a
		 * neighbor, if not send a connection request
		 */

		ChandyLamport serverObj = new ChandyLamport("server", nodePortMap.get(NodeId), "hostname");
		new Thread(serverObj).start();

		/* Iterate through the neighbor List */
		/*
		 * If the neighborId is lower than your NodeId, send a connection
		 * request
		 */
		for (String neighbor : rc.getNeighbors(NodeId)) {
			if (Integer.parseInt(NodeId) > Integer.parseInt(neighbor)) {
				/*
				 * Create a client object and send a connection request to this
				 * neighbor
				 */
				ChandyLamport clientObj = new ChandyLamport("client", rc.nodePortMap.get(neighbor),
						rc.nodeMachineMap.get(neighbor));
				new Thread(clientObj).start();

			} else {

				/*
				 * Check if the neighbor's neighbor List has you as a neighbor.
				 * If not you only send a connection request
				 */
				if (!rc.getNeighbors(neighbor).contains(NodeId)) {
					ChandyLamport clientObj = new ChandyLamport("client", rc.nodePortMap.get(neighbor),
							rc.nodeMachineMap.get(neighbor));
					new Thread(clientObj).start();
				}

			}
		}

		/*
		 * If the total number of connections is equal to number of neighbors,
		 * all neighbor connections are established
		 */

		while (true) {

			if (num_connections == rc.getNeighbors(NodeId).size()) {
//				System.out.println("All connections are established");
				break;
			} else {
//				System.out.println("Waiting for all neighbor nodes to be connected");
				Thread.sleep(1000);
			}

		}

		/* Creating a thread for each reader and read the messages */

		for (String hostname : readerMap.keySet()) {

			BufferedReader reader = readerMap.get(hostname);
			ChandyLamport readerThread = new ChandyLamport(reader, hostname, "read");
			new Thread(readerThread).start();
		}

		// System.out.println("random num of msgs: " + randMsgs);

		/* Select a random neighbor first */

		/* get the writer object for this neighbor from the hashmap */

		ChandyLamport writerThread = new ChandyLamport("write");
		new Thread(writerThread).start();

		/* Send few application messages before starting snapshot */
		// Thread.sleep(2000);

		/*
		 * If nodeId is 0, save the state of the system , ie. write the vector
		 * clock data to config out file and start the snapshot
		 */

		if (NodeId.equals("0")) {
			ChandyLamport snapShotStartThread = new ChandyLamport("snapshot");
			new Thread(snapShotStartThread).start();
		}

		ChandyLamport markerThread = new ChandyLamport("marker");
		new Thread(markerThread).start();

		ChandyLamport terminationDetection = new ChandyLamport("termination");
		new Thread(terminationDetection).start();

	}

}
