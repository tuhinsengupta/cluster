package org.tuhin.cluster;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.DatagramPacket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Logger;


public class ClusterService implements Runnable{




	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ClusterService.class);


	private static final String CLUSTER_SERVICE_MAIN_THREAD_NAME = "ClusterService-Main";
	private static final String CLUSTER_SERVICE_DISCOVER_THREAD_NAME = "ClusterService-Discover";
	private static final String CLUSTER_SERVICE_ANNOUNCE_THREAD_NAME = "ClusterService-Announce";
	private static final String CLUSTER_SERVICE_HEARTBEAT_THREAD_NAME = "ClusterService-HeartBeat";
	private static final String CLUSTER_SERVICE_CLIENT_PROCESSOR_THREAD_NAME = "ClusterService-ClientRequestProcessor";

	private static ClusterService instance = null;

	protected ClusterConfig config;


	private boolean ready = false;
	private boolean error = false;
	private Exception exception = null;
	private boolean leader = false;
	protected ClusterMember leadMember = null;

	private Set<ClusterMember> joinedMemebers = Collections.synchronizedSet(new HashSet<ClusterMember>());

	private Map<String,Map<Object,Object>> mapStore = Collections.synchronizedMap(new HashMap<String,Map<Object,Object>>());

	private Map<String,ThreadPoolExecutor> servicePool = Collections.synchronizedMap(new HashMap<String,ThreadPoolExecutor>());


	private transient Map<String, Future<Object>> _futureHold = new HashMap<String, Future<Object>>();


	private ServerSocket runningServerSocket = null;

	private Thread serverThread;
	private MemberDiscoverThread discoverThread;
	private Thread heartBeatThread;


	protected boolean stopRequested = false;

	private ExecutorService clientProcessingService;




	private ClusterService(ClusterConfig config) {
		this.config=config;
	}



	public static ClusterService getInstance(ClusterConfig config) throws ClusterServiceException {
		if (logger.isDebugEnabled()) {
			logger.debug("getInstance(ClusterConfig) - start");
		}


		if ( instance == null ){
			instance = new ClusterService(config);
			instance.start();
		}

		if (logger.isDebugEnabled()) {
			logger.debug("getInstance(ClusterConfig) - end");
		}
		return instance;
	}


	public void restart() throws ClusterServiceException, IOException{

		stop();
		start();

	}

	public void stop() throws IOException{

		ready = false;
		error=false;
		exception=null;
		leader=false;

		stopRequested = true;

		if (discoverThread != null) {
			discoverThread.setStop(true);
		}
		if ( heartBeatThread != null ){
			heartBeatThread.interrupt();
		}

		//Wait for Heartbeat an leader assign thread to finish

		try {
			Thread.sleep(config.getHeartBeatInterval());
		} catch (InterruptedException e) {
			logger.error("run()", e);
		}


		if ( serverThread != null ){
			serverThread.interrupt();
		}

		if ( runningServerSocket != null ){
			runningServerSocket.close();
		}

		runningServerSocket = null;
		serverThread = null;
		runningServerSocket = null;

		joinedMemebers.clear();

		clientProcessingService.shutdown();
		try {
			clientProcessingService.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}

		for(String name:servicePool.keySet()){
			shutdownNowImpl(name);
		}

		instance = null;
	}

	public void start() throws ClusterServiceException {
		if (logger.isDebugEnabled()) {
			logger.debug("start() - start");
		}

		clientProcessingService = Executors.newCachedThreadPool();

		stopRequested = false;

		leader = false;

		runningServerSocket = null;


		serverThread = new Thread(this,CLUSTER_SERVICE_MAIN_THREAD_NAME);
		serverThread.start();
		while (!ready){
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				logger.error("start()", e);
			}
		}
		if ( error ){
			throw new ClusterServiceException(exception);
		}

		discoverThread = new MemberDiscoverThread(CLUSTER_SERVICE_DISCOVER_THREAD_NAME, this);
		discoverThread.start();
		while (!discoverThread.isReady()){
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				logger.error("start()", e);
			}
		}
		if ( discoverThread.isError() ){
			throw new ClusterServiceException(discoverThread.getException());
		}


		//Wait for Lead Member Discovery
		while (leadMember == null){
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				logger.error("start()", e);
			}
		}
		
		if ( !leadMember.equals(findCurrent())) {
			boolean dataSynced = syncData(leadMember);
	
			if (logger.isDebugEnabled() && dataSynced) {
				logger.debug("Data synced for current node.");
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug("start() - end");
		}
	}



	public ClusterConfig getConfig() {
		return config;
	}



	public void run() {
		if (logger.isDebugEnabled()) {
			logger.debug("run() - start");
		}

		ServerSocket serverSocket = null;

		try {

			boolean currentNodeStarted = false;
			ClusterMember currentMember = config.getCurrentMember();
			if (!currentNodeStarted){

				if (logger.isDebugEnabled()) {
					logger.debug("This is curent node : " + currentMember );
					logger.debug("Node not started : " + currentMember );
				}

				//Start current
				int port = currentMember.getPort();

				try{
					serverSocket = new ServerSocket(port, config.getSocketBacklog());
					currentMember.setCurrent();
					currentMember.setStarted();
					joinedMemebers.add(currentMember);
					currentNodeStarted = true;
					if (logger.isDebugEnabled()) {
						logger.debug("Node Listener port strated : " + currentMember );
					}
				}catch(IOException e){
					//Socket is used
					logger.warn("Node Listener could not be started for : " + currentMember ,e);

				}
			}

			if ( serverSocket != null ){
				logger.info("Starting cluster member : " + serverSocket.getLocalSocketAddress());
				heartBeatThread = new Thread(new ClusterHeartBeatService(this),CLUSTER_SERVICE_HEARTBEAT_THREAD_NAME);
				heartBeatThread.start(); //discovery thread

				ready=true;
				error=false;
				runningServerSocket  = serverSocket;
				// Broadcast me to other nodes
				broadcastMyPresence(serverSocket.getLocalPort(), config.getMulticastGroup(), config.getMulticastPort());

				while(!isStopped()){

					if ( stopRequested ){
						logger.info("Stop requested, exiting...");
						break;
					}
					try{
						clientProcessingService.submit(new ClientRequestProcessor(runningServerSocket.accept(),this));
					}catch(Exception e){
						if ( !stopRequested ){
							logger.error("run()", e);
						}
					}finally{
					}
				}

			}else{
				ready = true;
				error = true;
				exception = new ClusterServiceException("All nodes already running or this is not a valid cluster node.");
			}

		} catch (Exception ex) {
			logger.error("run()", ex);

			ready=true;
			error=true;
			exception=ex;
			throw new RuntimeException(ex);
		}finally{
			if ( serverSocket != null){
				try {
					serverSocket.close();
				} catch (IOException e) {
					logger.error("run()",e);
				}
			}
		}



		if (logger.isDebugEnabled()) {
			logger.debug("run() - end");
		}
	}

	protected void addMemberToCluster(ClusterMember member) {

		if (logger.isDebugEnabled()) {
			logger.debug("Adding member : " + member);
		}


		RunStatus stat;
		try{
			stat = member.isRunning(config.getNetworkTimeout());
		}catch(IOException e){
			logger.warn("Could not get status of node " + member.toString() + ", assuming dead. [" +  e.getMessage() + "]");
			stat = new RunStatus(RunStatus.Status.NotRunning,-1,-1, UUID.randomUUID());
		}
		if ( stat.isRunning() ){
			if (logger.isDebugEnabled()) {
				logger.debug("Member is running, adding to cluster : " + member);
			}
			member.setStarted(stat.getStarted());
			if ( stat.isLeader()){
				member.setStartedAsLeader(stat.getStartedAsLeader());
				leadMember = member;
			}
			member.setId(stat.getId());

			joinedMemebers.add(member);
		}

	}

	private void broadcastMyPresence(int port, String group, int multicast_port) throws Exception {
		new Thread(() -> {
			while(!isStopped()) {
				int ttl = 1;
				try(MulticastSocket clientSocket = new MulticastSocket()){
					InetAddress multicastAddress = InetAddress.getByName(group);
			        NetUtils.setInterface(clientSocket, multicastAddress instanceof Inet6Address);
			        clientSocket.setLoopbackMode(false);
					byte[] sendData = ByteBuffer.allocate(4).putInt(port).array();
					DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, multicastAddress, multicast_port);
					clientSocket.setTimeToLive(ttl);
					clientSocket.send(sendPacket);
					Thread.sleep(1000);
				}catch(Exception e) {
				}
			}

		}
		, CLUSTER_SERVICE_ANNOUNCE_THREAD_NAME).start();

	}



	public ClusterMember findCurrent() {
		if (logger.isDebugEnabled()) {
			logger.debug("findCurrent() - start");
		}

		return config.getCurrentMember();

	}


	protected boolean syncData(ClusterMember member) {
		if (logger.isDebugEnabled()) {
			logger.debug("syncData(ClusterMember) - start");
		}
		if ( member.isCurrent()){
			return false;
		}

		try{
			SyncData data = member.getStoredData();
			if ( data == null ){
				if (logger.isDebugEnabled()) {
					logger.debug("syncData(ClusterMember) - end");
				}
				return false;
			}

			synchronized (mapStore) {

				mapStore.clear();
				mapStore.putAll(data.getMapStore());
			}

			synchronized (servicePool) {

				servicePool.clear();
				Map<String,Integer> syncedPool = data.getServicePools();
				for(String name : syncedPool.keySet()){
					int threadPoolSize = syncedPool.get(name);
					servicePool.put(name, new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()));
				}

			}
			if (logger.isDebugEnabled()) {
				logger.debug("syncData(ClusterMember) - end");
			}
			return true;

		}catch(Exception e){
			logger.error("syncData(ClusterMember)", e);

			if (logger.isDebugEnabled()) {
				logger.debug("syncData(ClusterMember) - end");
			}
			return false;
		}
	}



	@SuppressWarnings("unchecked")
	private Object performOp(ClusterMessage message) throws InterruptedException, ExecutionException, TimeoutException {
		if (logger.isDebugEnabled()) {
			logger.debug("performOp(ClusterMessage) - start");
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Performing operation : " + message.getOperation());
		}

		if (message.getOperation() == ClusterMessage.Operation.CreateMap ){

			if ( message.getArgs().size() != 1){
				throw new IllegalArgumentException("Invalid no of arguments for operation CreateMap");
			}
			createMapImpl((String)message.getArgs().get(0));

		}

		if (message.getOperation() == ClusterMessage.Operation.ClearMap ){
			if ( message.getArgs().size() != 1){
				throw new IllegalArgumentException("Invalid no of arguments for operation ClearMap");
			}
			clearMapImpl((String)message.getArgs().get(0));

		}

		if (message.getOperation() == ClusterMessage.Operation.PutIntoMap ){
			if ( message.getArgs().size() != 3){
				throw new IllegalArgumentException("Invalid no of arguments for operation PutIntoMap");
			}
			Object returnObject = putIntoMapImpl((String) message.getArgs()
					.get(0), message.getArgs().get(1), message.getArgs().get(2));
			if (logger.isDebugEnabled()) {
				logger.debug("Operation " + message.getOperation() + " returning the Object put into map");
			}
			return returnObject;
		}

		if (message.getOperation() == ClusterMessage.Operation.PutAllIntoMap ){
			if ( message.getArgs().size() != 2){
				throw new IllegalArgumentException("Invalid no of arguments for operation PutAllIntoMap");
			}
			Map<Object,Object> map = (Map<Object,Object>)message.getArgs().get(1);
			putAllIntoMapImpl((String)message.getArgs().get(0),map);

		}

		if (message.getOperation() == ClusterMessage.Operation.RemoveFromMap ){
			if ( message.getArgs().size() != 2){
				throw new IllegalArgumentException("Invalid no of arguments for operation RemoveFromMap");
			}
			Object returnObject = removeFromMapImpl((String) message.getArgs()
					.get(0), message.getArgs().get(1));
			if (logger.isDebugEnabled()) {
				logger.debug("Operation " + message.getOperation() + " returning the Object from map");
			}
			return returnObject;
		}

		if (message.getOperation() == ClusterMessage.Operation.CreateExecutorService ){
			if ( message.getArgs().size() != 2){
				throw new IllegalArgumentException("Invalid no of arguments for operation CreateExecutorService");
			}

			createExecutorServiceImpl((String)message.getArgs().get(0),(Integer)message.getArgs().get(1));

		}

		if (message.getOperation() == ClusterMessage.Operation.Execute ){
			if ( message.getArgs().size() != 2){
				throw new IllegalArgumentException("Invalid no of arguments for operation Execute");
			}

			executeImpl((String) message.getArgs().get(0), (Runnable) message.getArgs().get(1));
		}

		if (message.getOperation() == ClusterMessage.Operation.AwaitTermination ){
			if ( message.getArgs().size() != 3){
				throw new IllegalArgumentException("Invalid no of arguments for operation AwaitTermination");
			}
			Boolean returnObject;
			returnObject = awaitTerminationImpl((String) message.getArgs().get(0), (Long) message.getArgs().get(1), (TimeUnit) message.getArgs().get(2));
			return returnObject;
		}

		if (message.getOperation() == ClusterMessage.Operation.IsShutdown ){
			if ( message.getArgs().size() != 1){
				throw new IllegalArgumentException("Invalid no of arguments for operation IsShutdown");
			}
			Boolean returnObject;
			returnObject = isShutdownImpl((String) message.getArgs().get(0));
			return returnObject;
		}

		if (message.getOperation() == ClusterMessage.Operation.IsTerminated ){
			if ( message.getArgs().size() != 1){
				throw new IllegalArgumentException("Invalid no of arguments for operation IsTerminated");
			}
			Boolean returnObject;
			returnObject = isTerminatedImpl((String) message.getArgs().get(0));
			return returnObject;
		}

		if (message.getOperation() == ClusterMessage.Operation.ShutdownNow ){
			if ( message.getArgs().size() != 1){
				throw new IllegalArgumentException("Invalid no of arguments for operation ShutdownNow");
			}
			List<Runnable> returnObject;
			returnObject = shutdownNowImpl((String) message.getArgs().get(0));
			return returnObject;
		}

		if (message.getOperation() == ClusterMessage.Operation.Shutdown ){
			if ( message.getArgs().size() != 1){
				throw new IllegalArgumentException("Invalid no of arguments for operation Shutdown");
			}
			shutdownImpl((String) message.getArgs().get(0));
		}

		if (message.getOperation() == ClusterMessage.Operation.InvokeAll ){
			if ( message.getArgs().size() != 4){
				throw new IllegalArgumentException("Invalid no of arguments for operation InvokeAll");
			}
			return invokeAllImpl((String) message.getArgs().get(0),(Collection<? extends Callable<Object>>)message.getArgs().get(1), (Long) message.getArgs().get(2), (TimeUnit) message.getArgs().get(3));
		}

		if (message.getOperation() == ClusterMessage.Operation.Submit ){
			if ( message.getArgs().size() != 2){
				throw new IllegalArgumentException("Invalid no of arguments for operation Submit");
			}
			return submitImpl((String) message.getArgs().get(0),(Callable<Object>)message.getArgs().get(1));
		}

		if (message.getOperation() == ClusterMessage.Operation.InvokeAny ){
			if ( message.getArgs().size() != 4){
				throw new IllegalArgumentException("Invalid no of arguments for operation InvokeAny");
			}
			return invokeAnyImpl((String) message.getArgs().get(0),(Collection<? extends Callable<Object>>)message.getArgs().get(1), (Long) message.getArgs().get(2), (TimeUnit) message.getArgs().get(3));
		}

		if (message.getOperation() == ClusterMessage.Operation.Cancel ){
			if ( message.getArgs().size() != 2){
				throw new IllegalArgumentException("Invalid no of arguments for operation Cancel");
			}
			return cancelImpl((String) message.getArgs().get(0),(Boolean) message.getArgs().get(1));
		}


		if (message.getOperation() == ClusterMessage.Operation.Get ){
			if ( message.getArgs().size() != 3){
				throw new IllegalArgumentException("Invalid no of arguments for operation Get");
			}
			return getImpl((String) message.getArgs().get(0), (Long)message.getArgs().get(1), (TimeUnit)message.getArgs().get(2));
		}

		if (message.getOperation() == ClusterMessage.Operation.IsCancelled ){
			if ( message.getArgs().size() != 1){
				throw new IllegalArgumentException("Invalid no of arguments for operation IsCancelled");
			}
			return isCancelledImpl((String) message.getArgs().get(0));
		}

		if (message.getOperation() == ClusterMessage.Operation.IsDone ){
			if ( message.getArgs().size() != 1){
				throw new IllegalArgumentException("Invalid no of arguments for operation IsDone");
			}
			return isDoneImpl((String) message.getArgs().get(0));
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Operation " + message.getOperation() + " returning NULL");
		}
		return null;

	}



	private boolean cancelImpl(String futureInstance, boolean interruptIfRunning) {
		Future<Object> future = _futureHold.get(futureInstance);

		if ( future == null ){
			return true;
		}
		return future.cancel(interruptIfRunning);

	}

	private boolean isCancelledImpl(String futureInstance) {
		Future<Object> future = _futureHold.get(futureInstance);

		if ( future == null ){
			return true;
		}
		return future.isCancelled();

	}

	private boolean isDoneImpl(String futureInstance) {
		Future<Object> future = _futureHold.get(futureInstance);

		if ( future == null ){
			return true;
		}
		return future.isDone();

	}



	private Object getImpl(String futureInstance, long arg0, TimeUnit arg1) throws InterruptedException, ExecutionException, TimeoutException {
		Future<Object> future = _futureHold.get(futureInstance);

		if ( future == null ){
			throw new IllegalArgumentException("Futute not exists or get() already called.");
		}
		Object ret;
		if ( arg0 == -1){
			ret = future.get();
		}else{
			ret = future.get(arg0,arg1);;
		}

		_futureHold.remove(futureInstance);

		return ret;

	}


	private boolean awaitTerminationImpl(String name, long arg0, TimeUnit arg1) throws InterruptedException {

		ExecutorService pool = servicePool.get(name);

		if ( pool == null ){
			throw new IllegalArgumentException("no such ExecutorService pool : " + name);
		}

		return pool.awaitTermination(arg0, arg1);

	}

	private void executeImpl(String name, Runnable runnable) {

		ExecutorService pool = servicePool.get(name);

		if ( pool == null ){
			throw new IllegalArgumentException("no such ExecutorService pool : " + name);
		}

		pool.execute(runnable);

	}



	private List<DistributedFuture<Object>> invokeAllImpl(String name, Collection<? extends Callable<Object>> arg0, long arg2, TimeUnit arg3) throws InterruptedException {

		ExecutorService pool = servicePool.get(name);

		if ( pool == null ){
			throw new IllegalArgumentException("no such ExecutorService pool : " + name);
		}

		List<Future<Object>> futures;
		if ( arg2 == -1){
			futures = pool.invokeAll(arg0);
		}else{
			futures = pool.invokeAll(arg0,arg2, arg3);
		}

		List<DistributedFuture<Object>> returnVal = new ArrayList<DistributedFuture<Object>>();

		for (final Future<Object> future:futures){

			String id = UUID.randomUUID().toString();
			_futureHold.put(id, future);

			returnVal.add(new DistributedFuture<Object>(findCurrent(),id));
		}

		return returnVal;


	}

	private DistributedFuture<Object> submitImpl(String name, Callable<Object> arg0) throws InterruptedException {

		ExecutorService pool = servicePool.get(name);

		if ( pool == null ){
			throw new IllegalArgumentException("no such ExecutorService pool : " + name);
		}

		Future<Object> future =  pool.submit(arg0);
		String id = UUID.randomUUID().toString();
		_futureHold.put(id, future);


		return new DistributedFuture<Object>(findCurrent(),id);


	}



	private Object invokeAnyImpl(String name, Collection<? extends Callable<Object>> arg0, long arg2, TimeUnit arg3) throws InterruptedException, ExecutionException, TimeoutException {

		ExecutorService pool = servicePool.get(name);

		if ( pool == null ){
			throw new IllegalArgumentException("no such ExecutorService pool : " + name);
		}

		if ( arg2 == -1){
			return pool.invokeAny(arg0);
		}else{
			return pool.invokeAny(arg0,arg2, arg3);
		}

	}

	private void createExecutorServiceImpl(String name, int threadPoolSize) {

		ThreadPoolExecutor pool = servicePool.get(name);
		if ( pool == null ){
			pool = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

			servicePool.put(name, pool);


		}else{
			//return pool;
			//throw new IllegalArgumentException("ExecutorService pool already created : " + name);
		}

	}

	private boolean isShutdownImpl(String name)  {

		ExecutorService pool = servicePool.get(name);

		if ( pool == null ){
			throw new IllegalArgumentException("no such ExecutorService pool : " + name);
		}

		return pool.isShutdown();

	}

	private boolean isTerminatedImpl(String name) {

		ExecutorService pool = servicePool.get(name);

		if ( pool == null ){
			throw new IllegalArgumentException("no such ExecutorService pool : " + name);
		}

		return pool.isTerminated();

	}

	private void shutdownImpl(String name)  {

		ExecutorService pool = servicePool.get(name);

		if ( pool == null ){
			throw new IllegalArgumentException("no such ExecutorService pool : " + name);
		}

		pool.shutdown();

	}


	private List<Runnable> shutdownNowImpl(String name)  {

		ExecutorService pool = servicePool.get(name);

		if ( pool == null ){
			throw new IllegalArgumentException("no such ExecutorService pool : " + name);
		}

		return pool.shutdownNow();

	}




	private void createMapImpl(String uniqueName) {
		if (logger.isDebugEnabled()) {
			logger.debug("createMapImpl(String) - start");
		}

		synchronized (mapStore) {

			mapStore.put(uniqueName, new HashMap<Object, Object>());

		}

		if (logger.isDebugEnabled()) {
			logger.debug("createMapImpl(String) - end");
		}
	}


	private void clearMapImpl(String uniqueName) {
		if (logger.isDebugEnabled()) {
			logger.debug("clearMapImpl(String) - start");
		}

		synchronized (mapStore) {

			Map<Object, Object> m = mapStore.get(uniqueName);
			if (m != null ){
				synchronized (m) {

					m.clear();
				}
			}
		}


		if (logger.isDebugEnabled()) {
			logger.debug("clearMapImpl(String) - end");
		}
	}



	private Object putIntoMapImpl(String uniqueName,Object key,Object value) {
		if (logger.isDebugEnabled()) {
			logger.debug("putIntoMapImpl(String, Object, Object) - start");
		}

		if ( key == null ){
			if (logger.isDebugEnabled()) {
				logger.debug("putIntoMapImpl(String, Object, Object) - end");
			}
			return null;
		}
		synchronized (mapStore) {

			Map<Object, Object> m = mapStore.get(uniqueName);
			if (m != null ){
				synchronized (m) {

					Object returnObject = m.put(key, value);
					if (logger.isDebugEnabled()) {
						logger
						.debug("putIntoMapImpl(String, Object, Object) - end");
					}
					return returnObject;
				}
			}else{
				if (logger.isDebugEnabled()) {
					logger
					.debug("putIntoMapImpl(String, Object, Object) - end");
				}
				return null;
			}
		}


	}

	private void putAllIntoMapImpl(String uniqueName,Map<Object,Object> map) {
		if (logger.isDebugEnabled()) {
			logger.debug("putAllIntoMapImpl(String, Map) - start");
		}

		if ( map == null ){
			if (logger.isDebugEnabled()) {
				logger.debug("putAllIntoMapImpl(String, Map) - end");
			}
			return;
		}
		synchronized (mapStore) {

			Map<Object, Object> m = mapStore.get(uniqueName);
			if (m != null ){
				synchronized (m) {

					m.putAll(map);
				}
			}
		}


		if (logger.isDebugEnabled()) {
			logger.debug("putAllIntoMapImpl(String, Map) - end");
		}
	}


	private Object removeFromMapImpl(String uniqueName,Object key) {
		if (logger.isDebugEnabled()) {
			logger.debug("removeFromMapImpl(String, Object) - start");
		}

		if ( key == null ){
			if (logger.isDebugEnabled()) {
				logger.debug("removeFromMapImpl(String, Object) - end");
			}
			return null;
		}
		synchronized (mapStore) {

			Map<Object, Object> m = mapStore.get(uniqueName);
			if (m != null ){
				synchronized (m) {

					Object returnObject = m.remove(key);
					if (logger.isDebugEnabled()) {
						logger.debug("removeFromMapImpl(String, Object) - end");
					}
					return returnObject;
				}
			}else{
				if (logger.isDebugEnabled()) {
					logger.debug("removeFromMapImpl(String, Object) - end");
				}
				return null;
			}
		}


	}



	private List<Object> broadcast(final ClusterMessage message) throws IOException, InterruptedException{
		if (logger.isDebugEnabled()) {
			logger.debug("broadcast(ClusterMessage) - start");
		}

		Set<ClusterMember> members = getMembers() ;
		if ( members.size() > 0 ){
			ExecutorService broadcastService = Executors.newFixedThreadPool(members.size());

			Collection<Callable<ResultObject>> calls = new ArrayList<Callable<ResultObject>>();

			for ( final ClusterMember member : members ){

				Callable<ResultObject> call = new Callable<ResultObject>() {

					public ResultObject call()  {
						if(member.isCurrent()){
							try {
								return new ResultObject(performOp(message));
							} catch (InterruptedException e) {
								return new ResultObject(e);
							} catch (ExecutionException e) {
								return new ResultObject(e);
							} catch (TimeoutException e) {
								return new ResultObject(e);
							}
						}else{
							try {
								return member.sendMessage(message);
							} catch (IOException e) {
								return new ResultObject(e);
							} catch (ClassNotFoundException e) {
								return new ResultObject(e);
							}
						}
					}
				};

				calls.add(call);
			}


			try {
				List<Object> returnVals = new ArrayList<Object>();
				List<Future<ResultObject>> returns = broadcastService.invokeAll(calls);
				broadcastService.shutdown();
				boolean terminated_gracefully = broadcastService.awaitTermination(config.maxWait(), TimeUnit.MINUTES);
				if ( terminated_gracefully ){
					for(Future<ResultObject> returnObject : returns){
						if (returnObject.isDone()){
							ResultObject result = returnObject.get();
							if ( result.isSuccess() ){
								returnVals.add(result.getResult());
							}else{
								throw new IOException(result.getException());
							}
						}
					}
				}else{
					broadcastService.shutdownNow();
					throw new IOException("Threads not terminated after " + config.maxWait() + " minutes");
				}
				if (logger.isDebugEnabled()) {
					logger.debug("broadcast(ClusterMessage) - end");
				}
				return returnVals;

			}catch (ExecutionException e) {
				throw new IOException("Exception on task execution", e);
			}

		}else{
			return new ArrayList<Object>();
		}
	}

	public void createMap(String uniqueName) throws IOException, InterruptedException {
		if (logger.isDebugEnabled()) {
			logger.debug("createMap(String) - start");
		}

		if ( mapStore.get(uniqueName) == null ){
			broadcast(new ClusterMessage(ClusterMessage.Operation.CreateMap,uniqueName));
		}

		if (logger.isDebugEnabled()) {
			logger.debug("createMap(String) - end");
		}
	}

	public void createExecutorService(String instanceId, int threadPoolSize) throws IOException, InterruptedException {
		if (logger.isDebugEnabled()) {
			logger.debug("createExecutorService(String) - start");
		}

		if ( mapStore.get(instanceId) == null ){
			broadcast(new ClusterMessage(ClusterMessage.Operation.CreateExecutorService,instanceId,threadPoolSize));
		}

		if (logger.isDebugEnabled()) {
			logger.debug("createExecutorService(String) - end");
		}
	}


	public void clear(String uniqueName) throws IOException, InterruptedException {
		if (logger.isDebugEnabled()) {
			logger.debug("clear(String) - start");
		}

		broadcast(new ClusterMessage(ClusterMessage.Operation.ClearMap,uniqueName));

		if (logger.isDebugEnabled()) {
			logger.debug("clear(String) - end");
		}
	}

	public Object put(String uniqueName, Object key, Object value) throws IOException, InterruptedException {
		if (logger.isDebugEnabled()) {
			logger.debug("put(String, Object, Object) - start");
		}

		List<Object> returnObjects = broadcast(new ClusterMessage(
				ClusterMessage.Operation.PutIntoMap,
				uniqueName,
				key,
				value));
		if (logger.isDebugEnabled()) {
			logger.debug("put(String, Object, Object) - end");
		}
		if ( returnObjects.size() > 0 ){
			return returnObjects.get(0);
		}else{
			return null;
		}
	}

	public void putAll(String uniqueName, Map<Object,Object> map) throws IOException, InterruptedException {
		if (logger.isDebugEnabled()) {
			logger.debug("putAll(String, Map) - start");
		}

		broadcast(new ClusterMessage(ClusterMessage.Operation.PutAllIntoMap,uniqueName,map));

		if (logger.isDebugEnabled()) {
			logger.debug("putAll(String, Map) - end");
		}
	}

	public Object remove(String uniqueName, Object key) throws IOException, InterruptedException {
		if (logger.isDebugEnabled()) {
			logger.debug("remove(String, Object) - start");
		}

		List<Object> returnObjects = broadcast(new ClusterMessage(
				ClusterMessage.Operation.RemoveFromMap,
				uniqueName,
				key));
		if (logger.isDebugEnabled()) {
			logger.debug("remove(String, Object) - end");
		}
		if ( returnObjects.size() > 0 ){
			return returnObjects.get(0);
		}else{
			return null;
		}
	}


	public boolean containsKey(String uniqueName, Object key) {
		if (logger.isDebugEnabled()) {
			logger.debug("containsKey(String, Object) - start");
		}

		synchronized (mapStore) {

			Map<Object, Object> m = mapStore.get(uniqueName);
			if (m != null ){
				synchronized (m) {

					boolean returnboolean = m.containsKey(key);
					if (logger.isDebugEnabled()) {
						logger.debug("containsKey(String, Object) - end");
					}
					return returnboolean;
				}
			}else{
				if (logger.isDebugEnabled()) {
					logger.debug("containsKey(String, Object) - end");
				}
				return false;
			}
		}
	}



	public boolean containsValue(String uniqueName, Object value) {
		if (logger.isDebugEnabled()) {
			logger.debug("containsValue(String, Object) - start");
		}

		synchronized (mapStore) {

			Map<Object, Object> m = mapStore.get(uniqueName);
			if (m != null ){
				synchronized (m) {

					boolean returnboolean = m.containsValue(value);
					if (logger.isDebugEnabled()) {
						logger.debug("containsValue(String, Object) - end");
					}
					return returnboolean;
				}
			}else{
				if (logger.isDebugEnabled()) {
					logger.debug("containsValue(String, Object) - end");
				}
				return false;
			}
		}	
	}



	public  Set<java.util.Map.Entry<Object, Object>> entrySet(String uniqueName) {
		if (logger.isDebugEnabled()) {
			logger.debug("entrySet(String) - start");
		}

		synchronized (mapStore) {

			Map<Object, Object> m = mapStore.get(uniqueName);
			if (m != null ){
				synchronized (m) {

					Set<java.util.Map.Entry<Object, Object>> returnSet = m.entrySet();
					if (logger.isDebugEnabled()) {
						logger.debug("entrySet(String) - end");
					}
					return returnSet;
				}
			}else{
				Set<java.util.Map.Entry<Object, Object>> returnSet = new HashSet<java.util.Map.Entry<Object, Object>>();
				if (logger.isDebugEnabled()) {
					logger.debug("entrySet(String) - end");
				}
				return returnSet ;
			}
		}	
	}



	public Object get(String uniqueName, Object key) {
		if (logger.isDebugEnabled()) {
			logger.debug("get(String, Object) - start");
		}

		synchronized (mapStore) {
			Map<Object, Object> m = mapStore.get(uniqueName);
			if (m != null ){
				synchronized (m) {

					Object returnObject = m.get(key);
					if (logger.isDebugEnabled()) {
						logger.debug("get(String, Object) - end");
					}
					return returnObject;
				}
			}else{
				if (logger.isDebugEnabled()) {
					logger.debug("get(String, Object) - end");
				}
				return null ;
			}
		}	
	}

	public int size(String uniqueName) {
		if (logger.isDebugEnabled()) {
			logger.debug("size(String) - start");
		}

		synchronized (mapStore) {
			Map<Object, Object> m = mapStore.get(uniqueName);
			if (m != null ){
				synchronized (m) {

					int returnint = m.size();
					if (logger.isDebugEnabled()) {
						logger.debug("size(String) - end");
					}
					return returnint;
				}
			}else{
				if (logger.isDebugEnabled()) {
					logger.debug("size(String) - end");
				}
				return 0 ;
			}
		}	
	}



	public boolean isEmpty(String uniqueName) {
		if (logger.isDebugEnabled()) {
			logger.debug("isEmpty(String) - start");
		}

		synchronized (mapStore) {

			Map<Object, Object> m = mapStore.get(uniqueName);
			if (m != null ){
				synchronized (m) {

					boolean returnboolean = m.isEmpty();
					if (logger.isDebugEnabled()) {
						logger.debug("isEmpty(String) - end");
					}
					return returnboolean;
				}
			}else{
				if (logger.isDebugEnabled()) {
					logger.debug("isEmpty(String) - end");
				}
				return true;
			}
		}
	}


	public Set<Object> keySet(String uniqueName) {
		if (logger.isDebugEnabled()) {
			logger.debug("keySet(String) - start");
		}

		synchronized (mapStore) {

			Map<Object, Object> m = mapStore.get(uniqueName);
			if (m != null ){
				synchronized (m) {

					Set<Object> returnSet = m.keySet();
					if (logger.isDebugEnabled()) {
						logger.debug("keySet(String) - end");
					}
					return returnSet;
				}
			}else{
				Set<Object> returnSet = new HashSet<Object>();
				if (logger.isDebugEnabled()) {
					logger.debug("keySet(String) - end");
				}
				return returnSet ;
			}
		}	
	}



	public Collection<Object> values(String uniqueName) {
		if (logger.isDebugEnabled()) {
			logger.debug("values(String) - start");
		}

		synchronized (mapStore) {

			Map<Object, Object> m = mapStore.get(uniqueName);
			if (m != null ){
				synchronized (m) {

					Collection<Object> returnCollection = m.values();
					if (logger.isDebugEnabled()) {
						logger.debug("values(String) - end");
					}
					return returnCollection;
				}
			}else{
				Collection<Object> returnCollection = new ArrayList<Object>();
				if (logger.isDebugEnabled()) {
					logger.debug("values(String) - end");
				}
				return returnCollection;
			}
		}	
	}



	public boolean isLeader() {
		if (logger.isDebugEnabled()) {
			logger.debug("isLeader() - start");
		}

		if (logger.isDebugEnabled()) {
			logger.debug("isLeader() - end");
		}
		return leader;
	}



	public Set<ClusterMember> getMembers() {
		if (logger.isDebugEnabled()) {
			logger.debug("getMembers() - start");
		}

		Set<ClusterMember> members = Collections.synchronizedSet(new HashSet<ClusterMember>());
		synchronized (joinedMemebers) {

			for(ClusterMember member:joinedMemebers){
				members.add(member);
			}

		}
		if (logger.isDebugEnabled()) {
			logger.debug("getMembers() - end");
		}
		return members;
	}



	private ClusterMember getLeaderImpl()  {
		if (logger.isDebugEnabled()) {
			logger.debug("getLeaderImpl() - start");
		}

		if (logger.isDebugEnabled()) {
			logger.debug("getLeaderImpl() - end");
		}
		return leadMember;
	}

	public ClusterMember getLeader() throws ClusterServiceException {
		if (logger.isDebugEnabled()) {
			logger.debug("getLeader() - start");
		}

		ClusterMember member = getLeaderImpl();

		if ( member == null ){

			logger.info("No leader present - sleeping for " + (config.getWaitForLeaderInterval()*2) + " ms");
			try {
				Thread.sleep(config.getWaitForLeaderInterval()*2);
			} catch (InterruptedException e) {
				logger.error("getLeader()", e);
			}

			member = getLeaderImpl();
		}
		if (member == null ){
			throw new ClusterServiceException("No leader found");
		}

		if (logger.isDebugEnabled()) {
			logger.debug("getLeader() - end");
		}
		return member;
	}



	public Set<ClusterMember> getConfiguredMembers() {
		if (logger.isDebugEnabled()) {
			logger.debug("getConfiguredMembers() - start");
		}

		Set<ClusterMember> returnSet = getMembers();

		if (logger.isDebugEnabled()) {
			logger.debug("getConfiguredMembers() - end");
		}
		return returnSet;
	}



	public int getWaitForLeaderInterval() {
		if (logger.isDebugEnabled()) {
			logger.debug("getWaitForLeaderInterval() - start");
		}

		int returnint = config.getWaitForLeaderInterval();
		if (logger.isDebugEnabled()) {
			logger.debug("getWaitForLeaderInterval() - end");
		}
		return returnint;
	}



	public void execute(ClusterMember member, String instanceId, Runnable arg0) throws IOException {

		ClusterMessage message = new ClusterMessage(ClusterMessage.Operation.Execute,instanceId,arg0);
		if(member.isCurrent()){
			try {
				performOp(message);
			} catch (InterruptedException e) {
				throw new IOException(e);

			} catch (ExecutionException e) {
				throw new IOException(e);
			} catch (TimeoutException e) {
				throw new IOException(e);
			}
		}else{
			try {
				ResultObject result = member.sendMessage(message);
				if (!result.isSuccess()){
					throw new IOException(result.getException());
				}
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}



	}



	public boolean awaitTermination(String instanceId,	long arg0, TimeUnit arg1) throws IOException, InterruptedException {
		ClusterMessage message = new ClusterMessage(ClusterMessage.Operation.AwaitTermination,instanceId,arg0, arg1);

		List<Object> results = broadcast(message);

		boolean retVal = true;
		for(Object result:results){

			if ( result instanceof Boolean ){
				retVal = retVal && (Boolean)result;
			}else{
				retVal = false;

			}
			if ( retVal == false){
				break;
			}
		}

		return retVal;
	}

	private ResultObject performMessageOnMember(ClusterMember member, ClusterMessage message) throws IOException{
		ResultObject retVal = null;
		if(member.isCurrent()){
			try {
				retVal = new ResultObject(performOp(message));
			} catch (InterruptedException e) {
				retVal = new ResultObject(e);
			} catch (ExecutionException e) {
				retVal = new ResultObject(e);
			} catch (TimeoutException e) {
				retVal = new ResultObject(e);
			}
		}else{
			try {
				retVal = member.sendMessage(message);
			} catch (ClassNotFoundException e) {
				retVal = new ResultObject(e);
			}
		}

		return retVal;
	}

	@SuppressWarnings("unchecked")
	public <T> List<Future<T>> invokeAll(ClusterMember member, String instanceId,	Collection<? extends Callable<T>> arg0,long arg1, TimeUnit arg2) throws IOException, InterruptedException {
		ClusterMessage message = new ClusterMessage(ClusterMessage.Operation.InvokeAll,instanceId,arg0, arg1,arg2);

		ResultObject retVal = performMessageOnMember(member, message);

		if ( retVal.isSuccess() ){
			return (List<Future<T>>)retVal.getResult();
		}else{
			throw new IOException(retVal.getException());
		}
	}


	@SuppressWarnings("unchecked")
	public <T> T invokeAny(ClusterMember member, String instanceId,	Collection<? extends Callable<T>> arg0,long arg1, TimeUnit arg2) throws IOException, InterruptedException, ExecutionException, TimeoutException {
		ClusterMessage message = new ClusterMessage(ClusterMessage.Operation.InvokeAny,instanceId,arg0, arg1,arg2);

		ResultObject retVal = performMessageOnMember(member, message);

		if ( retVal.isSuccess() ){
			return (T)retVal.getResult();
		}else{
			throw new ExecutionException(retVal.getException());
		}
	}

	@SuppressWarnings("unchecked")
	public <T> Future<T> submit(ClusterMember member, String instanceId,	Callable<T> arg0) throws IOException, InterruptedException {
		ClusterMessage message = new ClusterMessage(ClusterMessage.Operation.Submit,instanceId,arg0);

		ResultObject retVal = performMessageOnMember(member, message);

		if ( retVal.isSuccess() ){
			return (Future<T>)retVal.getResult();
		}else{
			throw new IOException(retVal.getException());
		}
	}

	public boolean isShutdown(String instanceId) throws IOException, InterruptedException {
		ClusterMessage message = new ClusterMessage(ClusterMessage.Operation.IsShutdown,instanceId);

		List<Object> results = broadcast(message);

		boolean retVal = true;
		for(Object result:results){

			if ( result instanceof Boolean ){
				retVal = retVal && (Boolean)result;
			}else{
				retVal = false;

			}
			if ( retVal == false){
				break;
			}
		}

		return retVal;

	}

	public boolean isTerminated(String instanceId) throws IOException, InterruptedException {
		ClusterMessage message = new ClusterMessage(ClusterMessage.Operation.IsTerminated,instanceId);

		List<Object> results = broadcast(message);

		boolean retVal = true;
		for(Object result:results){

			if ( result instanceof Boolean ){
				retVal = retVal && (Boolean)result;
			}else{
				retVal = false;

			}
			if ( retVal == false){
				break;
			}
		}

		return retVal;

	}

	public void shutdown(String instanceId) throws IOException, InterruptedException {
		ClusterMessage message = new ClusterMessage(ClusterMessage.Operation.Shutdown,instanceId);

		broadcast(message);


	}

	@SuppressWarnings("unchecked")
	public List<Runnable> shutdownNow(String instanceId) throws IOException, InterruptedException {
		ClusterMessage message = new ClusterMessage(ClusterMessage.Operation.ShutdownNow,instanceId);

		List<Runnable> retVals = new ArrayList<Runnable>();

		List<Object> results = broadcast(message);

		for(Object result:results){

			if ( result instanceof List<?>){
				retVals.addAll((List<Runnable>)result);
			}else{
				throw new IOException("At least one shutdownNow call did not succeed");
			}
		}

		return retVals;

	}




	public boolean isStopped() {
		if ( runningServerSocket == null ){
			return true;
		}
		if ( runningServerSocket.isClosed() ){
			return true;
		}
		return false;
	}

	private final class ClientRequestProcessor implements Runnable {
		private final Socket clientSocket;
		private final ClusterService clusterService;

		private ClientRequestProcessor(Socket clientSocket,ClusterService clusterService) {
			this.clientSocket = clientSocket;
			this.clusterService = clusterService;
		}

		public void run() {
			Thread.currentThread().setName(CLUSTER_SERVICE_CLIENT_PROCESSOR_THREAD_NAME);
			ObjectInputStream ois = null;
			try{
				ois = new ObjectInputStream(clientSocket.getInputStream());
				ClusterMessage msg = (ClusterMessage)ois.readObject();

				if ( msg != null ){
					if ( msg.getOperation() == ClusterMessage.Operation.Sync ){


						if (logger.isDebugEnabled()) {
							logger.debug("Received SYNC Message.");
						}
						synchronized (mapStore) {

							ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
							oos.writeObject(new ResultObject(new SyncData(mapStore, servicePool)));
							oos.close();
						}

					}else if ( msg.getOperation() == ClusterMessage.Operation.UnsetLeader ){


						if (logger.isDebugEnabled()) {
							logger.debug("Received UnsetLeader Message.");
						}
						clusterService.setLeader(false);
						ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
						oos.writeObject(new ResultObject(null));
						oos.close();

					}else if ( msg.getOperation() == ClusterMessage.Operation.ZipLogFiles ){


						if (logger.isDebugEnabled()) {
							logger.debug("Received ZipLogFiles Message.");
						}try{
							File zipFile = clusterService.zipLogFiles((String)msg.getArgs().get(0),(String)msg.getArgs().get(1));
							byte[] bytes = new byte[16 * 1024];
							InputStream in = new FileInputStream(zipFile);
							OutputStream out = clientSocket.getOutputStream();

							int count;
							while ((count = in.read(bytes)) > 0) {
								out.write(bytes, 0, count);
							}

							out.close();
							in.close();
						}catch(Throwable t){
							logger.error("ClientRequestProcessor.run()", t);
							try{
								File zipFile = clusterService.createErrorZipFile(t);
								byte[] bytes = new byte[16 * 1024];
								InputStream in = new FileInputStream(zipFile);
								OutputStream out = clientSocket.getOutputStream();

								int count;
								while ((count = in.read(bytes)) > 0) {
									out.write(bytes, 0, count);
								}

								out.close();
								in.close();
							}catch(IOException e){
								logger.error("run()", e);
							}

						}

					}else if ( msg.getOperation() == ClusterMessage.Operation.Stat ){

						if (logger.isDebugEnabled()) {
							logger.debug("Received STAT Message.");
						}

						ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
						ClusterMember member = findCurrent();
						oos.writeObject(new ResultObject(new RunStatus(leader?RunStatus.Status.Leader:RunStatus.Status.Member, member.getStarted(), member.getStartedAsLead(),member.getId())));
						oos.close();

					}else	if ( msg.getOperation() == ClusterMessage.Operation.Custom ){

						if (logger.isDebugEnabled()) {
							logger.debug("Received Custom Op Message.");
						}

						if ( msg.getArgs().size() < 1){
							throw new IllegalArgumentException("Invalid number of arguments for Custom Operation");
						}

						CustomOperation custom = (CustomOperation)msg.getArgs().get(0);
						Object[] args = new Object[msg.getArgs().size() - 1];
						for(int i=0;i<msg.getArgs().size() - 1;i++){
							args[i] = msg.getArgs().get(i+1);
						}

						Object result = custom.execute(args);

						ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
						oos.writeObject(new ResultObject(result));
						oos.close();

					}else{

						if (logger.isDebugEnabled()) {
							logger.debug("Received OP Message.");
						}

						ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
						ResultObject result;
						try {
							result = new ResultObject(performOp(msg));
						} catch (InterruptedException e) {
							result = new ResultObject(e);
						} catch (ExecutionException e) {
							result = new ResultObject(e);
						} catch (TimeoutException e) {
							result = new ResultObject(e);
						} catch (Throwable e) {
							result = new ResultObject(e);
						}

						oos.writeObject(result);
						oos.close();
					}

				}
			}catch(Throwable t){
				logger.error("ClientRequestProcessor.run()", t);
				try{
					ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
					oos.writeObject(new ResultObject(t));
					oos.close();
				}catch(IOException e){
					logger.error("run()", e);

				}
			}finally{

				if ( clientSocket != null ){
					try{
						clientSocket.close();
					}catch(Exception e){
						logger.error("run()", e);

					}
				}

				if ( ois != null ){
					try{
						ois.close();
					}catch(Exception e){
						logger.error("run()", e);

					}
				}

			}
		}
	}

	public int getNetworkTimeout() {
		return config.getNetworkTimeout();
	}




	public void setLeader(boolean leader) {
		this.leader=leader;
	}



	public ClusterMember findMember(String ip, int port) {
		for(ClusterMember member : joinedMemebers){
			if (member.getAddress().getHostAddress().equals(ip) && member.getPort() == port){
				return member;
			}
		}
		return null;
	}



	public File zipLogFiles(String dir, final String fileName) throws IOException{
		try{
			File directory = new File(dir);

			if ( directory.isDirectory() ){
				String[] files = directory.list(new FilenameFilter() {

					public boolean accept(File file, String listedFile) {
						if ( listedFile.contains(fileName)){
							return true;
						}else{
							return false;
						}
					}
				});

				FileOutputStream fos = null;
				ZipOutputStream zos = null;

				try{
					File outputZip = File.createTempFile("logZip", ".zip");

					fos = new FileOutputStream(outputZip.getAbsolutePath());
					zos = new ZipOutputStream(fos);

					if ( files.length == 0 ){
						addNoFilesFoundToZipFile(dir,fileName, zos);
					}
					for(String file:files){
						addToZipFile(dir, file, zos);
					}

					outputZip.deleteOnExit();
					return outputZip;
				}finally{
					if ( zos != null ){
						zos.close();
					}
					if ( fos != null){
						fos.close();		
					}
				}

			}else{
				return createErrorZipFile("Not a directory : " + dir);
			}
		}
		catch(Throwable t){
			return createErrorZipFile(t);
		}
	}

	private void addToZipFile(String dir, String fileName, ZipOutputStream zos) throws IOException {

		FileInputStream fis = null;
		try{
			fis = new FileInputStream(dir + File.separator + fileName);
			ZipEntry zipEntry = new ZipEntry(fileName);
			zos.putNextEntry(zipEntry);

			byte[] bytes = new byte[1024];
			int length;
			while ((length = fis.read(bytes)) >= 0) {
				zos.write(bytes, 0, length);
			}

			zos.closeEntry();
		}finally{
			if ( fis !=null){
				fis.close();
			}
		}
	}

	private void addNoFilesFoundToZipFile(String dir, String fileName, ZipOutputStream zos) throws IOException {

		try{
			ZipEntry zipEntry = new ZipEntry("NoFilesFound.txt");
			zos.putNextEntry(zipEntry);

			byte[] bytes = ("No files found matching filename '" + fileName + "' on directory '" + dir + "'").getBytes();
			zos.write(bytes, 0, bytes.length);
			zos.closeEntry();
		}finally{
		}
	}

	private void addStackTraceToZipFile(Throwable t, ZipOutputStream zos) throws IOException {

		try{
			ZipEntry zipEntry = new ZipEntry("Error.txt");
			zos.putNextEntry(zipEntry);

			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			t.printStackTrace(pw);
			byte[] bytes = ("Exception received.\r\n" +  t.getMessage() + "\r\n" +  sw.toString() + "\r\n").getBytes();
			zos.write(bytes, 0, bytes.length);
			zos.closeEntry();
		}finally{
		}
	}

	private void addErrorToZipFile(String error, ZipOutputStream zos) throws IOException {

		try{
			ZipEntry zipEntry = new ZipEntry("Error.txt");
			zos.putNextEntry(zipEntry);
			byte[] bytes = (error + "\r\n").getBytes();
			zos.write(bytes, 0, bytes.length);
			zos.closeEntry();
		}finally{
		}
	}

	public File createErrorZipFile(Throwable t) throws IOException {


		FileOutputStream fos = null;
		ZipOutputStream zos = null;

		try{
			File outputZip = File.createTempFile("logZip", ".zip");

			fos = new FileOutputStream(outputZip.getAbsolutePath());
			zos = new ZipOutputStream(fos);

			addStackTraceToZipFile(t, zos);
			outputZip.deleteOnExit();
			return outputZip;
		}finally{
			if ( zos != null ){
				zos.close();
			}
			if ( fos != null){
				fos.close();		
			}
		}

	}

	public File createErrorZipFile(String error) throws IOException {


		FileOutputStream fos = null;
		ZipOutputStream zos = null;

		try{
			File outputZip = File.createTempFile("logZip", ".zip");

			fos = new FileOutputStream(outputZip.getAbsolutePath());
			zos = new ZipOutputStream(fos);

			addErrorToZipFile(error, zos);
			outputZip.deleteOnExit();
			return outputZip;
		}finally{
			if ( zos != null ){
				zos.close();
			}
			if ( fos != null){
				fos.close();		
			}
		}

	}



	public void removeMemberFromCluster(ClusterMember member) {
		joinedMemebers.remove(member);
	}


}
