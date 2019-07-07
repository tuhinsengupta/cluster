package org.tuhin.cluster;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.UUID;


public class ClusterMember implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4751190758756813889L;

	private static final int DEFAULT_PORT = 9200;
	
	private UUID id = UUID.randomUUID();

	private InetAddress address;
	private int port;
	private int weight;
	private long started = -1;
	private long startedAsLead = -1;
	public int getWeight() {
		return weight;
	}

	private transient boolean current = false;

	public void reset() {
	 started = -1;
	 startedAsLead = -1;
		
	}
	
	public void setStarted(){
		started = System.currentTimeMillis();
	}

	public void setStartedAsLead(){
		startedAsLead = System.currentTimeMillis();		
	}

	public long getStartedAsLead() {
		return startedAsLead;
	}

	public void setStartedAsLead(long startedAsLead) {
		this.startedAsLead = startedAsLead;
	}

	public long getStarted() {
		return started;
	}

	public ClusterMember(InetAddress address, int port, int weight) {
		super();
		this.address = address;
		this.port = port;
		this.weight = weight;
	}

	public ClusterMember(InetAddress address, int port) {
		this(address,port,1);
	}

	public ClusterMember(InetAddress address) {

		this(address, DEFAULT_PORT);
	}


	public ClusterMember() throws UnknownHostException {

		this(InetAddress.getLocalHost(), DEFAULT_PORT);
	}
	public InetAddress getAddress() {
		return address;
	}
	public int getPort() {
		return port;
	}

	public RunStatus isRunning(int timeout) throws IOException {
		try{
			try{
				if (!address.isReachable(timeout)){
					return new RunStatus(RunStatus.Status.NotRunning,-1,-1, UUID.randomUUID());
				}
			}catch(IOException e){
				//ignore this and rely on sendMessage API below
			}

			ResultObject result = sendMessage(new ClusterMessage(ClusterMessage.Operation.Stat));
			if ( result.isSuccess()  ){
				return (RunStatus)result.getResult();
			}else{
				throw new IOException(result.getException());
			}
		}catch (ClassNotFoundException e) {
			throw new RuntimeException(e);	
		}
	}


	public boolean isCurrent() {
		return current;
	}
	public ClusterMember setCurrent(){
		current = true;
		return this;
	}


	public SyncData getStoredData() throws IOException, ClassNotFoundException {

		ResultObject result = sendMessage(new ClusterMessage(ClusterMessage.Operation.Sync));


		if ( result.isSuccess()){
			SyncData data = (SyncData)result.getResult();

			return data;

		}else{
			throw new IOException(result.getException());
		}

	}

	@Override
	public String toString() {
		return "[" + address + ":" + port + (current?" this]" : "]");
	}

	public String simpleString() {
		return address.getHostName() + ":" + port;
	}

	public ResultObject sendMessage(ClusterMessage message) throws IOException, ClassNotFoundException {

		Socket socket = null;
		ObjectOutputStream oos = null;
		ObjectInputStream ois= null;
		try{
			socket = new Socket(getAddress(), getPort());

			oos = new ObjectOutputStream(socket.getOutputStream());

			oos.writeObject(message);

			oos.flush();

			if ( message.getOperation() == ClusterMessage.Operation.ZipLogFiles){
				File outputZip = File.createTempFile("logZip", ".zip");

				InputStream in = null;
				OutputStream out = null;
				try{
					in = socket.getInputStream();

					out = new FileOutputStream(outputZip);

					byte[] bytes = new byte[16*1024];

					int count;
					int total = 0;
					while ((count = in.read(bytes)) > 0) {
						out.write(bytes, 0, count);
						total += count;
					}
					if ( total == 0 ){
						throw new IOException("Zero byte file received.");						
					}else{
						return new ResultObject(outputZip);
					}
				}catch(Exception e){
					return new ResultObject(e);

				}finally{
					if ( out != null){
						out.close();
					}
					if ( in != null ){
						in.close();
					}
					outputZip.deleteOnExit();
				}

			}else{
				ois = new ObjectInputStream(socket.getInputStream());
				return (ResultObject)ois.readObject();
			}

		}finally{
			if ( socket != null ){
				try {
					socket.close();
				} catch (IOException e) {
				}
			}
			if ( oos != null ){
				try {
					oos.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public void setStarted(long started) {
		this.started = started;
	}

	public void setStartedAsLeader(long startedAsLeader) {
		this.startedAsLead = startedAsLeader; 
		
	}

	public ClusterMember deepCopy() {
		ClusterMember newCopy = new ClusterMember(address, port, weight);
		newCopy.setStarted(started);
		newCopy.setStartedAsLeader(startedAsLead);
		return newCopy;
	}

	public static ClusterMember allocateLocal(InetAddress localHost, int weight) throws IOException{
		try(ServerSocket s = new ServerSocket(0)){
			int localPort = s.getLocalPort();
			return new ClusterMember(localHost, localPort, weight).setCurrent();
		}
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
		
	}




}
