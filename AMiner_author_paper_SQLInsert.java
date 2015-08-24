package Extractdata;

import java.io.BufferedReader;
import java.util.Date;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class AMiner_author_paper_SQLInsert {
static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
static final String DB_URL = "jdbc:mysql://localhost:3306/aminer";

static final String USER = "root";
static final String PASS = "sakuntala";

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
        ResultSet tables=dbm.getTables(null, null, "temp_AMiner_author_paper", null);
        if(tables.next())
        	System.out.println("Table present......");
        else
        {
        	System.out.println("Table created......");
        	stmt = conn.createStatement();
        	        	
        	sql = "CREATE TABLE IF NOT EXISTS temp_AMiner_author_paper ("+
        			"ap_index bigint(15) DEFAULT NULL,"+
        			"a_index bigint(15) DEFAULT NULL,"+
        			"p_index bigint(15) DEFAULT NULL,"+
        			"author_position bigint(15) DEFAULT NULL )";
        	stmt.executeUpdate(sql);
        }	
        String sCurrentLine = "";     
        in = new BufferedReader(new FileReader("F:/work/Aminer/AMiner-Author2Paper.txt"));       
   	    
   	    int ap_index =0, a_index =0, p_index =0, author_position=0,field=0;
   	    a=0;
   	    String wrd="";
   	    //StringTokenizer stk = null;
   	    int totwrd=0,p1;
   	    char ch;
   	    //while((sCurrentLine = in.readLine())!=null && sCurrentLine.length()!=0)
   	 while((sCurrentLine = in.readLine())!=null)
    	{
   	                //stk = new StringTokenizer(sCurrentLine);
   	    			String st1="";
   	    			i=sCurrentLine.length();
   	    			for(p1=0;p1<i;p1++)
   	    				{
   	    				ch=sCurrentLine.charAt(p1);
   	    				if(ch != '\t')
   	    					st1+=ch;
   	    				else
   	    					{
   	    					if((p1+1)<i)
   	    						{
   	    						if(sCurrentLine.charAt(p1+1) != ' ')
   	    							st1=st1+" ";   
   	    						}
   	    					}
   	    				}
   	    			sCurrentLine=st1+" ";
   	    			totwrd=0;
   	    			field=0;
   	    			for(p1=0;p1<sCurrentLine.length();p1++)
	    				{
	    				ch=sCurrentLine.charAt(p1);
	    				if(ch == ' ')
	    					totwrd++;
	    				}
   	    			//System.out.println("===>"+totwrd);
   	    			//System.out.println(sCurrentLine);
   	    			for( p1=0;p1<sCurrentLine.length();p1++)
   	    			{
   	    				ch=sCurrentLine.charAt(p1);
   	    				if(ch != ' ')
   	    					wrd+=ch;
   	    				else
   	    				{
   	    					field++;
   	    					if(totwrd == 4)
   	    					{
   	    					if(field == 1)
   	    						ap_index = Integer.parseInt(wrd);
   	    					else if(field == 2)
   	    						a_index = Integer.parseInt(wrd);
   	    					else if(field == 3)
   	    						p_index = Integer.parseInt(wrd);
   	    					else if(field == 4)
   	    					    author_position = Integer.parseInt(wrd);
   	    					}
   	    					else if(totwrd == 3)
   	    					{
   	    					if(field == 1)
   	    						ap_index = Integer.parseInt(wrd);
   	    					else if(field == 2)
   	    						a_index = Integer.parseInt(wrd);
   	    					else if(field == 3)
   	    						p_index = Integer.parseInt(wrd);
   	    					else if(field == 4)
   	    					    author_position = 0;
   	    					}
   	    					else if(totwrd == 2)
   	    					{
   	    					if(field == 1)
   	    						ap_index = Integer.parseInt(wrd);
   	    					else if(field == 2)
   	    						a_index = Integer.parseInt(wrd);
   	    					else if(field == 3)
   	    						p_index = 0;
   	    					else if(field == 4)
   	    					    author_position = 0;
   	    					}
   	    					else if(totwrd == 1)
   	    					{
   	    					if(field == 1)
   	    						ap_index = Integer.parseInt(wrd);
   	    					else if(field == 2)
   	    						a_index = 0;
   	    					else if(field == 3)
   	    						p_index = 0;
   	    					else if(field == 4)
   	    					    author_position = 0;
   	    					}
   	    					wrd="";
   	    				}
   	    			}
   	    			//System.out.println(ap_index);
   	    			//System.out.println(a_index);
   	    			//System.out.println(p_index);
   	    			//System.out.println(author_position);
   	    			
                    if(ap_index>5149520)
                    {
                    	sql = "INSERT INTO temp_AMiner_author_paper (ap_index, a_index, p_index, author_position)" +
                            "VALUES (?, ?, ?, ?)";
               
        			PreparedStatement preparedStatement = conn.prepareStatement(sql);
        			preparedStatement.setInt(1, ap_index);
        			preparedStatement.setInt(2, a_index);
        			preparedStatement.setInt(3, p_index);
        			preparedStatement.setInt(4, author_position);
        			preparedStatement.executeUpdate();
                    }
                  
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