package Extractdata;


import java.util.*; 
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

public class Author_1st_poistion
{
	
	public static void main(String[] args) throws FileNotFoundException, IOException
    	{
	          Connection conn = null;
		  Statement stmt = null;
		  Statement stmt1 = null;
		  Statement stmt2 = null;
		  ResultSet rs = null;
		  ResultSet rs1 = null;
		  ResultSet rs2 = null;
		  
		  FileWriter writer = new FileWriter("F:/work/Aminer/21th_july/21thjuly_p1.txt",true); 
		  
		  String url = "jdbc:mysql://localhost:3306/";
		  String dbName = "aminer";
		  String driver = "com.mysql.jdbc.Driver";
		  String userName = "root"; 
		  String password = "sakuntala";
		  String res="";
		  try {
		  Class.forName(driver).newInstance();
		  conn = DriverManager.getConnection(url+dbName,userName,password);
		  System.out.println("Connected....");
		  
		  stmt = conn.createStatement();
	      	  stmt1 = conn.createStatement();
	          stmt2 = conn.createStatement();
	          String a_name="",sql,sql1,sql2;
	          String a_affiliations="",a_keyterm="";
	          int a_id=0,a_pub_count=0,a_citations_count=0,a_h_index_count=0;
	          float a_p_index_count=0;
	          String res1="";
                  int i,j,i1;
                  int singleaut=0;
                  int year=0;
                  String p_authors="";
                  String p_venue="";
                  int flag;
                  String fwrd="",lwrd="",afwrd="",alwrd="";
          Scanner s = new Scanner(new File("F:/work/Aminer/17th_july/author_id list.txt"));
          String reco="";
          while (s.hasNext()){
        	  reco=s.next();
          }
          s.close();

          ArrayList<String> flist = new ArrayList<String>(Arrays.asList(reco.split(",")));
          
	      int j1;
	      int firstpos;
	      //for( j1=0;j1<flist.size();j1++)
	      for( j1=6010;j1<flist.size();j1++)
	      {  
	      i1=Integer.parseInt(flist.get(j1));
	      singleaut=0;
          firstpos=0;
	      sql = "SELECT * FROM aminer_author_paper where a_index="+i1;
	      sql2 = "SELECT * FROM aminer_author where a_index="+i1;
	      rs = stmt.executeQuery(sql);
	      rs2 = stmt2.executeQuery(sql2);
	      
	         
	      while(rs2.next())
	      {
	    	  a_name = rs2.getString("a_name");
	    	  a_id = rs2.getInt("a_index");
	      }
	      
	      while(rs.next()){
	    	    res="";
	            int p_index = rs.getInt("p_index");
	            sql1 = "SELECT * FROM aminer_paper where p_index="+p_index;
	            rs1=stmt1.executeQuery(sql1);
	          
	            flag=0;
	            while(rs1.next())
	            {
	        	  p_authors=rs1.getString("p_authors");
	        	  p_authors=p_authors.replaceAll(";", " , ");
	        	  ArrayList<String> temp_p_authors = new ArrayList<String>(Arrays.asList(p_authors.split(",")));
	        	  
	        	  String wrd=temp_p_authors.get(0).trim();
	        	  
	        	  if(wrd.indexOf(' ')!= -1)
	        		  {
	        		  fwrd=wrd.trim().substring(0, wrd.trim().indexOf(' '));
	        		  lwrd=wrd.trim().substring(wrd.trim().lastIndexOf(' ')+1);
	        		  }
                  
	        	  if(a_name.trim().indexOf(' ')!= -1)
	        	  	{
	        		  afwrd=a_name.trim().substring(0, a_name.trim().indexOf(' '));
	                  alwrd=a_name.trim().substring(a_name.trim().lastIndexOf(' ')+1);
	        	  	}
	        	  
                  int a_name_comma_chk=a_name.indexOf(' ');
                  int wrd_comma_chk=wrd.indexOf(' ');
	        	  
	        	  if(temp_p_authors.size() == 1)
	        	  {
	        		  if((wrd.compareToIgnoreCase(a_name.trim()) == 0) || 
	      	        	    (fwrd.compareToIgnoreCase(a_name.substring(0, a_name.indexOf(' '))) == 0) ||
	      	        		(lwrd.compareToIgnoreCase(a_name.substring(a_name.lastIndexOf(' ')+1)) == 0))
	      	        		  {
	      	        		  singleaut++;
	      	        		  }
	        	  }
	        	  else
	        	  {
	        		  if((a_name.trim().compareToIgnoreCase(wrd) == 0) || 
	                         (fwrd.compareToIgnoreCase(afwrd) == 0) ||
	 	        		     (lwrd.compareToIgnoreCase(alwrd) == 0))
	 	        		      		firstpos++;
	        	  }
	        	  temp_p_authors.clear();
	          }
	          
	          
	         }
	         writer.write(a_id+"\t"+a_name+"\t"+singleaut+"\t"+firstpos+"\n");
                
             System.out.println("Compleate for =>"+i1+"\trecord no =>"+j1);
         //  System.out.println();
          
          System.gc(); 
	      }
	      conn.close();
	      writer.close();
	     
		  } catch (Exception e) {
		  e.printStackTrace();
		  }
	}

}
