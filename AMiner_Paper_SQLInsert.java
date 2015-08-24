package Extractdata;

import java.io.BufferedReader;
import java.util.Date;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class AMiner_Paper_SQLInsert {
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
        ResultSet tables=dbm.getTables(null, null, "AMiner_Paper1", null);
        if(tables.next())
        	System.out.println("Table present......");
        else
        {
        	System.out.println("Table created......");
        	stmt = conn.createStatement();
        	sql = "CREATE TABLE IF NOT EXISTS AMiner_Paper1 ("+
        			"p_index bigint(15) NOT NULL,"+
        			"p_title varchar(510) DEFAULT NULL,"+
        			"p_authors varchar(1250) DEFAULT NULL,"+
        			"p_affiliation varchar(7250) DEFAULT NULL,"+
        			"p_year int(15) NOT NULL,"+
        			"p_venue varchar(500) DEFAULT NULL,"+
        			"p_id_ref varchar(6900) DEFAULT NULL,"+
        			"p_abstract varchar(31825) DEFAULT NULL,"+
        			"PRIMARY KEY (p_index))";
        	stmt.executeUpdate(sql);
        }	
        	String sCurrentLine;     
        in = new BufferedReader(new FileReader("D:/surajit/Output 26thMay 2015/AMiner-Paper.txt"));       
   	    int c,f1,f2,f3,f4,f5,f6,f7,f8;
        String p_title="",p_authors="",p_affiliation="",p_venue="",p_id_ref="",p_abstract="";
        int p_index=0,p_year=0;
        for( a=0; a<2092356; a++)
        //for( a=0; a<20; a++)
                   {
                    c=0;
                    f1=f2=f3=f4=f5=f6=f7=f8=0;
                    p_index=0;
                    p_year=0;
                    p_title="";
                    p_authors="";
                    p_affiliation="";
                    p_venue="";
                    p_id_ref="";
                    p_abstract="";
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
                                    p_index=Integer.parseInt(sCurrentLine.substring(i+1,p));
                                
                                else if(sCurrentLine.charAt(1) == '*')
                                    p_title+=sCurrentLine.substring(i+1,p)+" ";
                                
                                else if(sCurrentLine.charAt(1) == '@')
                                    p_authors+=sCurrentLine.substring(i,p)+" ";
                                
                                else if(sCurrentLine.charAt(1) == 'o')
                                    p_affiliation+=sCurrentLine.substring(i+1,p)+" ";
                                
                                else if(sCurrentLine.charAt(1) == 't')
                                	{
                                    if(f5 == 0)
                                    	{
                                    	f5=1;
                                    	if(sCurrentLine.substring(i+1,p).length() == 0)
                                        	p_year=0;
                                        else
                                    		p_year=Integer.parseInt(sCurrentLine.substring(i+1,p));
                                    	}
                                    }    
                                else if(sCurrentLine.charAt(1) == 'c')
                                    p_venue+=sCurrentLine.substring(i,p)+" ";
                                
                                else if (sCurrentLine.charAt(1) == '%')
                                    p_id_ref+=sCurrentLine.substring(i,p)+" ";
                                
                                else if( sCurrentLine.charAt(1) == '!')
                                    p_abstract+=sCurrentLine.substring(i,p)+" ";
                                }
                            else
                                break;

                        }while(true);
                   // if(a>2075081)
                    if(a>2070338)
                    {
                    System.out.println(a);
                    sql = "INSERT INTO AMiner_Paper1 (p_index, p_title, p_authors, p_affiliation, p_year, p_venue, p_id_ref, p_abstract)" +
                                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                    PreparedStatement preparedStatement = conn.prepareStatement(sql);
                    preparedStatement.setInt(1, p_index);
                    preparedStatement.setString(2, p_title);
                    preparedStatement.setString(3, p_authors);
                    preparedStatement.setString(4, p_affiliation);
                    preparedStatement.setInt(5, p_year);
                    preparedStatement.setString(6, p_venue);
                    preparedStatement.setString(7, p_id_ref);
                    preparedStatement.setString(8, p_abstract);
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