package Extractdata;

import java.io.BufferedReader;
import java.util.Date;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class AMiner_coauthor_SQLInsert {
static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
static final String DB_URL = "jdbc:mysql://localhost:3306/aminer";

static final String USER = "root";
static final String PASS = "surajit";

public static void main(String[] args) {
    Date date = new Date();
    Connection conn = null;
    Statement stmt = null;
    BufferedReader in = null;        
    int a,p,i;
    Scanner scn = new Scanner(System.in);
    try {
    	String sql;
    	Class.forName("com.mysql.jdbc.Driver");
    	System.out.println("\nConnecting to database...");
        conn = DriverManager.getConnection(DB_URL, USER, PASS);
        
        DatabaseMetaData dbm=conn.getMetaData();
        ResultSet tables=dbm.getTables(null, null, "AMiner_coauthor", null);
        if(tables.next())
        	System.out.println("Table present......");
        else
        {
        	System.out.println("Table created......");
        	stmt = conn.createStatement();
        	        	
        	sql = "CREATE TABLE IF NOT EXISTS AMiner_coauthor ("+
        			"c_author_1 int(15) DEFAULT NULL,"+
        			"c_author_2 int(15) DEFAULT NULL,"+
        			"c_collabor_cou int(5) NOT NULL)";
        	stmt.executeUpdate(sql);
        }	
        	String sCurrentLine;     
        in = new BufferedReader(new FileReader("D:/surajit/Output 26thMay 2015/AMiner-Coauthor.txt"));
        //in = new BufferedReader(new FileReader("Z:/SURAJIT/26thMay 2015/AMiner-coauthor.txt"));       
   	    a=0;
   	    int c_author_1=0,c_author_2,c_collabor_cou;
   	    while((sCurrentLine = in.readLine())!=null && sCurrentLine.length()!=0)
   	    //for( a=0; a<100; a++)
                   {
   	    			//a=a+1;                   
                    c_author_1=0;
                    c_author_2=0;
                    c_collabor_cou=0;
        			
        			StringTokenizer stk=new StringTokenizer(sCurrentLine);
        			//System.out.println(stk.nextToken());
        			//System.out.println(stk.nextToken());
        			//System.out.println(stk.nextToken());
        			String s1,s2,s3;
        			if(stk.countTokens()==3)
        			{
        				s1=stk.nextToken();
        				s2=stk.nextToken();
        				s3=stk.nextToken();
        				c_author_1=Integer.parseInt(s1.substring(1));
        				c_author_2=Integer.parseInt(s2);
        				c_collabor_cou=Integer.parseInt(s3);
        			}
        			else if(stk.countTokens()== 2)
        			{
        				s1=stk.nextToken();
        				s2=stk.nextToken();
        				s3="";
        				c_author_1=Integer.parseInt(s1.substring(1));
                        c_author_2=Integer.parseInt(s2);
                        c_collabor_cou=0;
        			}
        			else if(stk.countTokens()== 1)
        			{
        				s1=stk.nextToken();
        				s2="";
        				s3="";
        				c_author_1=Integer.parseInt(s3.substring(1));
                        c_author_2=0;
                        c_collabor_cou=0;
        			}
        			else
        			{
        				s1="";
        				s2="";
        				s3="";
        				c_author_1=0;
                        c_author_2=0;
                        c_collabor_cou=0;
        			}
        			//if(a>3824585)
        			//{
        			System.out.println(s1+"\t"+s2+"\t"+s3);
        			sql = "INSERT INTO AMiner_coauthor (c_author_1, c_author_2, c_collabor_cou)" +
                            "VALUES (?, ?, ?)";
               
        			PreparedStatement preparedStatement = conn.prepareStatement(sql);
        			preparedStatement.setInt(1, c_author_1);
        			preparedStatement.setInt(2, c_author_2);
        			preparedStatement.setInt(3, c_collabor_cou);
        			preparedStatement.executeUpdate();
        			//sCurrentLine = in.readLine();
        			//}
                    }       
        System.out.println("Date and time =>"+date.toString());
        System.out.println(" SUCCESS!\n");
        System.out.println("Total update record =>"+(a-1));

    }catch (NumberFormatException ne)
    {
        ne.printStackTrace();
    }
    catch(SQLException se) {
        se.printStackTrace();
    } catch(Exception e) {
        e.printStackTrace();
    } 
    finally {
        try {
            if(stmt != null)
                stmt.close();
        } catch(SQLException se) {
        }
        try {
            if(conn != null)
                conn.close();
        } catch(SQLException se) {
            se.printStackTrace();
        }
        try 
        {
            if (in != null)in.close();
        } 
        catch (IOException ex) 
        {
            ex.printStackTrace();
        }
    }
    
  }
}