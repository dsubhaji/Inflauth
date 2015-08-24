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
import java.util.Date;
import java.util.Locale;

public class new_version_Author_data_extract
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
		  
		  FileWriter writer = new FileWriter("F:/work/Aminer/16th july/output.txt",true); 
		  //CsvWriter csvOutput = new CsvWriter(new FileWriter("F:/15th July/output1.csv", true), ',');
		  
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
	      int index_code[]={
	    		  //157,342,551
	    		  //744495,449671,1077167,107495,218640,490333,1636029,667903,68402,894787,697678,1154517,623410,986281,805022,503060,1290664,1406932,1691054,1375963,823000,1631983,540922,209637,53643,82815,1380185,908569,90779
	    		  //2797,3050,4600  	
	    		  //157,342,551,863,940,1006,1651,2121,2451,2797,3050,4600
	    		  //863
	    		  };
	      for( i1=0;i1<index_code.length;i1++)
	      {  
	      singleaut=0;
	      sql = "SELECT * FROM aminer_author_paper where a_index="+index_code[i1];
	      sql2 = "SELECT * FROM aminer_author where a_index="+index_code[i1];
	      rs = stmt.executeQuery(sql);
	      rs2 = stmt2.executeQuery(sql2);
	      
	      ArrayList publication_year = new ArrayList();
	      ArrayList publication_venue = new ArrayList();
	      HashSet<String> uniqueSet_venue = new HashSet<String>();
	      HashSet<String> uniqueSet = new HashSet<String>();
	         
	      while(rs2.next())
	      {
	    	  a_name = rs2.getString("a_name");
	    	  a_id = rs2.getInt("a_index");
	    	  a_pub_count = rs2.getInt("a_pub_count");
	    	  a_citations_count = rs2.getInt("a_citations_count");
	    	  a_h_index_count = rs2.getInt("a_h_index_count");
	    	  a_p_index_count = rs2.getFloat("a_p_index_equal");
	    	  a_affiliations = rs2.getString("a_affiliations");
	    	  a_keyterm = rs2.getString("a_keyterms");
	      }
	      
	      while(rs.next()){
	    	  res="";
	          int a_index  = rs.getInt("a_index");
	          int p_index = rs.getInt("p_index");
	          System.out.println("p_index =>"+p_index);
	          sql1 = "SELECT * FROM aminer_paper a where p_index="+p_index;
	          rs1=stmt1.executeQuery(sql1);
	          System.out.println("Author name =>"+a_name);
	          while(rs1.next())
	          {
	        	  p_authors=rs1.getString("p_authors");
	        	  p_venue=rs1.getString("p_venue");
	        	  year=rs1.getInt("p_year");
	        	  
	        	  if(p_authors.trim().compareToIgnoreCase(a_name.trim()) == 0)
	        		  singleaut++;
	        	  else
	        		  {
	        		  p_authors=p_authors.replaceAll(";", " , ");
	        		  res=res+p_authors+",";
	        		  }
	          }
	          //res=res.substring(0,res.length()-1);
	          ArrayList<String> temp_list = new ArrayList<String>(Arrays.asList(res.split(",")));
	          System.out.println("temp_list"+temp_list);
	        		  
	          for(int index=0; index < temp_list.size(); index++)
	        			  {
	        			  String st=temp_list.get(index);
	        			  if(st.trim().compareToIgnoreCase(a_name.trim()) == 0)
	        				  {
	        				  temp_list.remove(index);
	        				  }
	        			  }
	        		         		  
	         System.out.println("p_author =>"+temp_list);
	         publication_year.add(year);
	         publication_venue.add(p_venue);
	         uniqueSet.addAll(temp_list);
	         temp_list.clear();
	         }
	          
	         uniqueSet_venue.addAll(publication_venue);
	         Collections.sort(publication_year);
	  
	         writer.write(a_id+"\t"+a_name+"\t"+a_affiliations+"\t"+a_pub_count+"\t"+a_citations_count+"\t"+a_h_index_count+"\t"+a_p_index_count+"\t"+a_keyterm+"\t"+uniqueSet.size()+"\t"+singleaut+"\t"+uniqueSet_venue.size()+"\t"+publication_year.get(0)+"\t"+publication_year.get(publication_year.size()-1)+"\n");
             String temp="";
             int totrow=0;
      
             uniqueSet.clear();
             publication_year.clear();
             publication_venue.clear();
             
                
          System.out.println("Compleate for =>"+index_code[i1]);
          System.out.println();
          
          System.gc(); 
	      }
	      conn.close();
	      writer.close();
	      
		  } catch (Exception e) {
		  e.printStackTrace();
		  }
	}

}



