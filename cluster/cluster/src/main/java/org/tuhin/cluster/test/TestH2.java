package org.tuhin.cluster.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TestH2 {

	public static void main(String[] args) throws ClassNotFoundException, SQLException{
		
		Class.forName("org.h2.Driver");
		
		Connection conn = DriverManager.getConnection("jdbc:h2:C:\\Users\\sentuh\\h2db");
		
		Statement stmt = conn.createStatement();
		
		stmt.execute("CREATE TABLE MONITOR_LOG(TASK VARCHAR(64), LOG VARCHAR(255))");
		
	}
}
