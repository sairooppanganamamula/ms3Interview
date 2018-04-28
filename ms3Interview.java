

package data;

import java.io.BufferedReader;
import java.sql.PreparedStatement;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
 
public class Databasetest {
	public int received=0;
	public int success=0;
	public int fail=0;
	Date date = new Date() ;
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss") ;
	
    public void createNewDatabase(String fileName) throws SQLException {
 
        String url = "jdbc:sqlite:C:\\Sai\\" + fileName;
 
        Connection conn = DriverManager.getConnection(url);
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
            }
 
        } 
    public void createNewTable(String database,String table) throws SQLException {
        String url = "jdbc:sqlite:C:\\Sai\\"+database;
        String sql = "CREATE TABLE"+" "+ table+"(\n"
                + "	A text,\n"
                + "	B text,\n"
                + "	C text,\n"
                + "	D text,\n"
                + "	E text,\n"
                + "	F text,\n"
                + "	G text,\n"
                + "	H text,\n"
                + "	I text,\n"
                + "	J text\n"
                + ");";      
        Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement() ;
            stmt.execute(sql);
        } 
    public void writeToTable(String Database,String tablename) throws SQLException, IOException
    {
    	File file = new File("C:\\Sai\\"+dateFormat.format(date) + ".csv") ;
		BufferedWriter out = new BufferedWriter(new FileWriter(file));
    	BufferedReader br = new BufferedReader(new FileReader("C:\\Sai\\ms3Interview.csv"));
    	String line;
    	int i=0;
    	while ( (line=br.readLine()) != null)
    	{
    		received++;
    	         String[] values =  line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    	         Connection conn = DriverManager.getConnection("jdbc:sqlite:C:\\Sai\\" + Database);
    	         Statement stat = conn.createStatement();
    	         String sql = "INSERT INTO"+" "+ tablename+" "+"(A,B,C,D,E,F,G,H,I,J) VALUES(?,?,?,?,?,?,?,?,?,?)";
    	         PreparedStatement pstmt = conn.prepareStatement(sql);
    	         if(values.length==10)
    	         {
    	        	 success++;
    	        	 pstmt.setString(1, values[0]);
    	        	 pstmt.setString(2, values[1]); 
    	        	 pstmt.setString(3, values[2]);
    	        	 pstmt.setString(4, values[3]);
    	        	 pstmt.setString(5, values[4]);
    	        	 pstmt.setString(6, values[5]);
    	        	 pstmt.setString(7, values[6]);
    	        	 pstmt.setString(8, values[7]);
    	        	 pstmt.setString(9, values[8]);
    	        	 pstmt.setString(10, values[9]);
    	             pstmt.executeUpdate();
    	             i++;
    	             System.out.println(i);
    	         }
    	         else
    	         {
    	        		out.write(line);
    	        		out.write("/n");
    	        	 fail++;
    	         }
    	     
    	}
    	out.close();
    }
    public void writeLog() throws IOException
    {
    	File file = new File("C:\\Sai\\stats.log") ;
		BufferedWriter out = new BufferedWriter(new FileWriter(file));
		out.write("total number of lines read:"+received );
		out.write("/n");
		out.write("number of lines read successfully:"+success);
		out.write("/n");
		out.write("number of lines failed to be read:"+fail);
		out.close();
    	
    }
 
    public static void main(String[] args) throws SQLException, IOException {
    	Databasetest db= new Databasetest();
        db.createNewDatabase("test03.db");
        db.createNewTable("test03.db","customer5");
        db.writeToTable("test03.db","customer5");
        db.writeLog();
        
    }
}