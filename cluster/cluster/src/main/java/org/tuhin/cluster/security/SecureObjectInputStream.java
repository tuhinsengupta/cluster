package org.tuhin.cluster.security;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

import org.tuhin.cluster.ClusterMember;
import org.tuhin.cluster.ClusterService;

public class SecureObjectInputStream implements Closeable{

	private ObjectInputStream iis;
	private ClusterMember sentFrom;

	public SecureObjectInputStream(InputStream is) throws IOException {
		iis = new ObjectInputStream(is);
	}

	
	
	@Override
	public void close() throws IOException {
		if (iis != null) {
			iis.close();
		}
	}



	public ClusterMember getSentFrom() {
		return sentFrom;
	}



	public Object readObject(ClusterService service) throws ClassNotFoundException, IOException {
		SecureMessageObject object = (SecureMessageObject) iis.readObject();
		sentFrom = object.getSentFrom();
		return object.getPlainObject(service);
	}

}
