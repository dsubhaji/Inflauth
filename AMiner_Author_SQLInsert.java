package Extractdata;

import java.io.BufferedReader;
import java.util.Date;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class AMiner_Author_SQLInsert {
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
        ResultSet tables=dbm.getTables(null, null, "AMiner_Author", null);
        if(tables.next())
        	System.out.println("Table present......");
        else
        {
        	System.out.println("Table created......");
        	stmt = conn.createStatement();
        	
        	sql = "CREATE TABLE IF NOT EXISTS AMiner_Author ("+
        			"a_index int(15) NOT NULL,"+
        			"a_name varchar(5000) DEFAULT NULL,"+
        			"a_affiliations varchar(15000) DEFAULT NULL,"+
        			"a_pub_count int(15) NOT NULL,"+
        			"a_citations_count int(15) NOT NULL,"+
        			"a_h_index_count int(15) NOT NULL,"+
        			"a_p_index_equal float(15,6) NOT NULL,"+
        			"a_u_p_index_uneq float(15,6) NOT NULL,"+
        			"a_keyterms text DEFAULT NULL,"+
        			"PRIMARY KEY (a_index))";
        	stmt.executeUpdate(sql);
        }	
        	String sCurrentLine;     
        in = new BufferedReader(new FileReader("D:/surajit/Output 26thMay 2015/AMiner-Author.txt"));       
   	    int c,f1,f2,f3,f4,f5,f6,f7,f8;
   	    
   	    int a_index=0,a_pub_count,a_citations_count,a_h_index_count;
   	    Float a_p_index_equal,a_u_p_index_uneq;
		String a_name,a_affiliations,a_keyterms;
        for( a=0; a<1712433; a++)
		           {
			
                    
                    c=0;
                    f1=f2=f3=f4=f5=f6=f7=f8=0;
                    
                    a_index=0;
        			a_name="";
        			a_affiliations="";
        			a_pub_count=0;
        			a_citations_count=0;
        			a_h_index_count=0;
        			a_p_index_equal=0.0f;
        			a_u_p_index_uneq=0.0f;
        			a_keyterms="";
        			
        			do
                        {
                            sCurrentLine = in.readLine();
                            p = 0;                        
                            p=sCurrentLine.length();
                            if(p!=0)
                            {
                                for(i=0;i<p;i++)
                                {
                                if(sCurrentLine.charAt(i)==' ')
                                    break;
                                }
                                if(sCurrentLine.charAt(1) == 'i')
                                	a_index=Integer.parseInt(sCurrentLine.substring(i+1,p));
                                
                                else if(sCurrentLine.charAt(1) == 'n')
                                	a_name+=sCurrentLine.substring(i+1,p)+" ";
                                
                                else if(sCurrentLine.charAt(1) == 'a')
                                	a_affiliations+=sCurrentLine.substring(i+1,p)+" ";
                                
                                else if(sCurrentLine.substring(1,3).equals("pc") == true)
                                	{
                                	//System.out.println("===>"+sCurrentLine.substring(0,3));
                                	if(sCurrentLine.substring(i+1,p).length() == 0)
                                		a_pub_count=0;
                                	else
                                		a_pub_count=Integer.parseInt(sCurrentLine.substring(i+1,p));
                                	}
                                
                                else if(sCurrentLine.charAt(1) == 'c')
                            	{
                            	if(sCurrentLine.substring(4,p).length() == 0)
                            		a_citations_count=0;
                            	else
                            		a_citations_count=Integer.parseInt(sCurrentLine.substring(4,p));
                            	}
                            
                                else if(sCurrentLine.charAt(1) == 'h')
                            	{
                            	if(sCurrentLine.substring(4,p).length() == 0)
                            		a_h_index_count=0;
                            	else
                            		a_h_index_count=Integer.parseInt(sCurrentLine.substring(4,p));
                            	}
                                
                                else if(sCurrentLine.substring(1,3).equals("pi") == true)
                            	{
                            	if(sCurrentLine.substring(4,p).length() == 0)
                            		a_p_index_equal=0.0f;
                            	else
                            		a_p_index_equal=Float.parseFloat(sCurrentLine.substring(4,p));
                            	}
                                
                                else if(sCurrentLine.charAt(1) == 'u')
                            	{
                            	if(sCurrentLine.substring(4,p).length() == 0)
                            		a_u_p_index_uneq=0.0f;
                            	else
                            		a_u_p_index_uneq=Float.parseFloat(sCurrentLine.substring(4,p));
                            	}
                                else if(sCurrentLine.charAt(1) == 't')
                                	a_keyterms+=sCurrentLine.substring(i,p)+" ";
                                }
                            else
                                break;

                        }while(true);
                   // if(a>1142588)
                   // if(a>1656564)
                   // {
                    	System.out.println(a);
        			//System.out.println("a_index=>"+a_index);
        		//	System.out.println("a_name=>"+a_name);
        			//System.out.println("a_affiliations=>"+a_affiliations);
        		//	System.out.println("a_pub_count=>"+a_pub_count);
        			//System.out.println("a_citations_count=>"+a_citations_count);
        		//	System.out.println("a_h_index_count=>"+a_h_index_count);
        			//System.out.println("a_p_index_equal=>"+a_p_index_equal);
        	//		System.out.println("a_u_p_index_uneq=>"+a_u_p_index_uneq);
        		//	System.out.println("a_keyterms=>"+a_keyterms);
        			//System.out.println();
        			
        			sql = "INSERT INTO AMiner_Author (a_index, a_name, a_affiliations, a_pub_count, a_citations_count, a_h_index_count, a_p_index_equal, a_u_p_index_uneq, a_keyterms)" +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
               
        			PreparedStatement preparedStatement = conn.prepareStatement(sql);
        			preparedStatement.setInt(1, a_index);
        			preparedStatement.setString(2, a_name);
        			preparedStatement.setString(3, a_affiliations);
        			preparedStatement.setInt(4, a_pub_count);
        			preparedStatement.setInt(5, a_citations_count);
        			preparedStatement.setInt(6, a_h_index_count);
        			preparedStatement.setFloat(7, a_p_index_equal);
        			preparedStatement.setFloat(8, a_u_p_index_uneq);
        			preparedStatement.setString(9, a_keyterms);
        			preparedStatement.executeUpdate();
                    
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