package org.tuhin.cluster;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.tuhin.cluster.security.PublicKeys;
import org.tuhin.cluster.security.SecureObjectInputStream;
import org.tuhin.cluster.security.SecureObjectOutputStream;
import org.tuhin.cluster.security.SecurityProtocol;


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

	private transient PrivateKey RSAPrivateKey;
	private byte[] RSAPublicKey;

	private transient PrivateKey DSAPrivateKey;
	private byte[] DSAPublicKey;

	private transient ClusterService service;

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

	public ClusterMember(ClusterService service, InetAddress address, int port, int weight) throws ClusterServiceException {
		super();

		if (service == null) {
			throw new ClusterServiceException("Service can not be null");
		}
		this.address = address;
		this.port = port;
		this.weight = weight;
		this.service = service;

		try {
			/* Generate  RSA key pair */
			KeyPairGenerator kpg = KeyPairGenerator.getInstance(SecurityProtocol.ALGO_RSA);
			kpg.initialize(SecurityProtocol.RSA_KEYSIZE);
			KeyPair kp = kpg.generateKeyPair();
			RSAPrivateKey = kp.getPrivate();
			RSAPublicKey = kp.getPublic().getEncoded();

			/* Generate  DSA key pair */
			KeyPairGenerator kpg1 = KeyPairGenerator.getInstance(SecurityProtocol.ALGO_DSA);
			kpg1.initialize(SecurityProtocol.DSA_KEYSIZE);
			KeyPair kp1 = kpg1.generateKeyPair();
			DSAPrivateKey = kp1.getPrivate();
			DSAPublicKey = kp1.getPublic().getEncoded();
		}catch(NoSuchAlgorithmException e) {
			throw new ClusterServiceException(e);
		}


	}

	public PrivateKey getRSAPrivateKey() {
		return RSAPrivateKey;
	}

	public PublicKey getRSAPublicKey() throws InvalidKeySpecException, NoSuchAlgorithmException {
		return KeyFactory.getInstance(SecurityProtocol.ALGO_RSA).generatePublic(new X509EncodedKeySpec(RSAPublicKey));
	}

	public byte[] getRSAPublicKeyBytes(){
		return RSAPublicKey;
	}


	public PrivateKey getDSAPrivateKey() {
		return DSAPrivateKey;
	}

	public PublicKey getDSAPublicKey() throws InvalidKeySpecException, NoSuchAlgorithmException {
		return KeyFactory.getInstance(SecurityProtocol.ALGO_DSA).generatePublic(new X509EncodedKeySpec(DSAPublicKey));
	}

	public byte[] getDSAPublicKeyBytes() {
		return DSAPublicKey;
	}

	public ClusterMember(ClusterService service, InetAddress address, int port) throws ClusterServiceException {
		this(service, address,port,1);
	}

	public ClusterMember(ClusterService service, InetAddress address) throws ClusterServiceException {

		this(service, address, DEFAULT_PORT);
	}


	public ClusterMember(ClusterService service) throws UnknownHostException, ClusterServiceException {

		this(service, InetAddress.getLocalHost(), DEFAULT_PORT);
	}
	public InetAddress getAddress() {
		return address;
	}
	public int getPort() {
		return port;
	}

	public RunStatus isRunning(int timeout) throws IOException {
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
	}

	@SuppressWarnings("unchecked")
	public Set<ClusterMember> getMembers(int timeout, Set<ClusterMember> members) throws IOException {
		try{
			if (!address.isReachable(timeout)){
				return new HashSet<ClusterMember>();
			}
		}catch(IOException e){
			//ignore this and rely on sendMessage API below
		}

		ResultObject result = sendMessage(new ClusterMessage(ClusterMessage.Operation.MemberList, members));
		if ( result.isSuccess()  ){
			return (Set<ClusterMember>)result.getResult();
		}else{
			throw new IOException(result.getException());
		}
	}

	public void setMembers(int timeout, Set<ClusterMember> members) throws IOException {
		try{
			if (!address.isReachable(timeout)){
				return;
			}
		}catch(IOException e){
			//ignore this and rely on sendMessage API below
		}

		ResultObject result = sendMessage(new ClusterMessage(ClusterMessage.Operation.SendMemberList, members));
		if ( result.isSuccess()  ){
			return;
		}else{
			throw new IOException(result.getException());
		}
	}


	public void setAddress(InetAddress address) {
		this.address = address;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public void setRSAPrivateKey(PrivateKey RSAPrivateKey) {
		this.RSAPrivateKey = RSAPrivateKey;
	}

	public void setRSAPublicKey(byte[] RSAPublicKey) {
		this.RSAPublicKey = RSAPublicKey;
	}

	public void setDSAPrivateKey(PrivateKey DSAPrivateKey) {
		this.DSAPrivateKey = DSAPrivateKey;
	}

	public void setDSAPublicKey(byte[] DSAPublicKey) {
		this.DSAPublicKey = DSAPublicKey;
	}

	public void setCurrent(boolean current) {
		this.current = current;
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

	public ResultObject sendMessage(ClusterMessage message) throws IOException {

		Socket socket = null;
		SecureObjectOutputStream oos = null;
		SecureObjectInputStream ois= null;
		try{
			socket = new Socket(getAddress(), getPort());

			oos = new SecureObjectOutputStream(socket.getOutputStream());

			oos.writeObject(service.getCurrent(), this, message);

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
				ois = new SecureObjectInputStream(socket.getInputStream());
				return (ResultObject)ois.readObject(service);
			}

		}catch(Exception e) {
			throw new IOException(e);
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
			if ( ois != null ){
				try {
					ois.close();
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

	public ClusterMember deepCopy() throws ClusterServiceException {
		ClusterMember newCopy = new ClusterMember(service, address, port, weight);
		newCopy.setStarted(started);
		newCopy.setStartedAsLeader(startedAsLead);
		newCopy.setId(id);
		newCopy.setAddress(address);
		newCopy.setCurrent(current);
		newCopy.setDSAPrivateKey(DSAPrivateKey);
		newCopy.setDSAPublicKey(DSAPublicKey);
		newCopy.setRSAPrivateKey(RSAPrivateKey);
		newCopy.setRSAPublicKey(RSAPublicKey);
		newCopy.setWeight(weight);
		return newCopy;
	}

	public static ClusterMember allocateLocal(ClusterService service, InetAddress localHost, int weight, int port) throws IOException, ClusterServiceException{
		try(ServerSocket s = new ServerSocket(port)){
			int localPort = s.getLocalPort();
			return new ClusterMember(service, localHost, localPort, weight).setCurrent();
		}
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;

	}

	public void setService(ClusterService service) {
		this.service = service;
	}

	public void updateActualKeys(int networkTimeout) throws IOException {
		DSAPrivateKey = null;
		DSAPublicKey = null;
		RSAPrivateKey = null;
		RSAPublicKey = null;
		ResultObject result = sendMessage(new ClusterMessage(ClusterMessage.Operation.GetKeys));
		if ( result.isSuccess()  ){
			PublicKeys keys =  (PublicKeys)result.getResult();
			RSAPublicKey = keys.getRSAPublicKey();
			DSAPublicKey = keys.getDSAPublicKey();
		}else{
			throw new IOException(result.getException());
		}
	}




}
