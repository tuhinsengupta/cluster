package org.tuhin.cluster.security;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.tuhin.cluster.ClusterMember;

public class SecureObjectOutputStream implements Closeable{

	private ObjectOutputStream oos;
	
	public SecureObjectOutputStream(OutputStream os) throws IOException, SecurityException {
		oos = new ObjectOutputStream(os);
	}
	public void writeObject(ClusterMember sendingFrom, ClusterMember toMember, Object object) throws IOException {
		if (oos != null ) {
			oos.writeObject(new SecureMessageObject(sendingFrom, toMember, object));
		}
	}
	

	public void close() throws IOException {
		if ( oos != null) {
			oos.close();
		}
		
	}
	public void flush() throws IOException {
		if ( oos != null) {
			oos.flush();
		}
	}

}
