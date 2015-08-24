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

public class new_version_Author_data_extract1
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

		  //FileWriter writer = new FileWriter("F:/work/Aminer/17th_july/output.txt",true);
		  FileWriter writer = new FileWriter("F:/work/Aminer/21th_july/21thjuly.txt",true);
		//  FileWriter writer = new FileWriter("F:/work/Aminer/17th_july/19thjuly_temp.txt",true);

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

          Scanner s = new Scanner(new File("F:/work/Aminer/17th_july/author_id list.txt"));
          String reco="";
          while (s.hasNext()){
        	  reco=s.next();
          }
          s.close();
          ArrayList<String> flist = new ArrayList<String>(Arrays.asList(reco.split(",")));

	      int j1;
	      for( j1=0;j1<flist.size();j1++)
	      //record process upto 3415
	      //record no =>3454
	      //for( j1=6000;j1<flist.size();j1++)
	      {
	      i1=Integer.parseInt(flist.get(j1));
	   //   System.out.println("i1=>"+i1);
	      singleaut=0;
	      sql = "SELECT * FROM aminer_author_paper where a_index="+i1;
	      sql2 = "SELECT * FROM aminer_author where a_index="+i1;
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
	  //    System.out.println("Author index =>"+a_id);
          System.out.println("Author name =>"+a_name);

	      while(rs.next()){
	    	  res="";
	          int a_index  = rs.getInt("a_index");
	          int p_index = rs.getInt("p_index");
	    //      System.out.println("p_index =>"+p_index);
	          sql1 = "SELECT * FROM aminer_paper a where p_index="+p_index;
	          rs1=stmt1.executeQuery(sql1);

	          flag=0;
	          while(rs1.next())
	          {
	        	  p_authors=rs1.getString("p_authors");
	        	  p_authors=p_authors.replaceAll(";", " , ");
	        	  int comma_chk=p_authors.indexOf(',');
	        	  p_venue=rs1.getString("p_venue");
	        	  year=rs1.getInt("p_year");
	       // 	  System.out.println("p_authors=>"+p_authors);
	        	//  System.out.println("comma_chk=>"+comma_chk);
	        	  if(comma_chk == -1 && ((p_authors.trim().compareToIgnoreCase(a_name.trim()) == 0) ||
	        			  (p_authors.trim().substring(0, p_authors.trim().indexOf(' ')).compareToIgnoreCase(a_name.substring(0, a_name.indexOf(' '))) == 0) ||
	        			  (p_authors.trim().substring(p_authors.trim().lastIndexOf(' ')+1).compareToIgnoreCase(a_name.substring(a_name.lastIndexOf(' ')+1)) == 0))
	        			  )
	        		  {
	        		  singleaut++;
	        		  flag=1;
	        		  }
	        	  else
	        		  {
	        		 // p_authors=p_authors.replaceAll(";", " , ");
	        	//	  System.out.println("test....");
	        		  res=res+p_authors+",";
	        		  }
	          }
	          //res=res.substring(0,res.length()-1);
	       //   System.out.println("res==>"+res);
	         //if(res.length() != 0)
	        	 ArrayList<String> temp_list = new ArrayList<String>(Arrays.asList(res.split(",")));


	          System.out.println("before delete temp_list"+temp_list);

	          for(int index=0; index < temp_list.size() && flag==0; index++)
	        			  {
	        			  String st=temp_list.get(index);
	        			  st=st.trim();
	        		//	  System.out.println("st=>"+st);
	        			  int onewrd=0;
	        			  if(st.indexOf(' ') != -1)
	        				  onewrd=1;
	        		//	  System.out.println("onewrd=>"+onewrd);
	        			//  String fst=st.substring(0, st.indexOf(' '));
	        			//  String lst=st.substring(st.lastIndexOf(' ')+1);

	        			  int onewrd_a_name=0;
	        			  a_name=a_name.trim();
	        			  if(a_name.indexOf(' ') != -1)
	        				  onewrd_a_name=1;
	        			//  System.out.println("onewrd_a_name=>"+onewrd_a_name);
	        			//  System.out.println("a_name=>"+a_name);
	        		//	  String fa_name=a_name.substring(0, a_name.indexOf(' '));
	        		//	  String la_name=a_name.substring(a_name.lastIndexOf(' ')+1);


	        			  //System.out.println("===>"+st+"\t"+fa_name+"\t"+fst+"\t"+la_name+"\t"+lst);
	        			  if(st.compareToIgnoreCase(a_name) == 0)
	        				  {
	        				  temp_list.remove(index);
	        				  }
	        			  else if(onewrd_a_name==1 && onewrd==1 &&((st.substring(0, st.indexOf(' ')).compareToIgnoreCase(a_name.substring(0, a_name.indexOf(' '))) == 0) ||
	        					  (st.substring(st.lastIndexOf(' ')+1).compareToIgnoreCase(a_name.substring(a_name.lastIndexOf(' ')+1)) == 0))
	        					  )
	        			  	  {
	        				  temp_list.remove(index);
	        			 	  }
	        			  else if(onewrd_a_name==1 && onewrd==0 &&((st.compareToIgnoreCase(a_name.substring(0, a_name.indexOf(' '))) == 0) ||
	        					  (st.compareToIgnoreCase(a_name.substring(a_name.lastIndexOf(' ')+1)) == 0))
	        					  )
	        			  	  {
	        				  temp_list.remove(index);
	        			 	  }
	        			  else if(onewrd_a_name==0 && onewrd==1 &&((st.substring(0, st.indexOf(' ')).compareToIgnoreCase(a_name) == 0) ||
	        					  (st.substring(st.lastIndexOf(' ')+1).compareToIgnoreCase(a_name) == 0))
	        					  )
	        			  	  {
	        				  temp_list.remove(index);
	        			 	  }
	        			  }

	         System.out.println("after delete p_author =>"+temp_list);
	       //  System.out.println("year=>"+year);
	      //   System.out.println("p_venue=>"+p_venue);
	        // System.out.println("temp_list =>"+temp_list.get(0).length());
	     //    System.out.println("temp_list =>"+temp_list.size());

	         publication_year.add(year);
	         publication_venue.add(p_venue);


	         if(temp_list.size() != 0 && temp_list.get(0).length() != 0)
	        	 uniqueSet.addAll(temp_list);
	         temp_list.clear();
	         }

	         uniqueSet_venue.addAll(publication_venue);
	         Collections.sort(publication_year);


	    //     System.out.println("unique p_author =>"+uniqueSet);

	         writer.write(a_id+"\t"+a_name+"\t"+a_affiliations+"\t"+a_pub_count+"\t"+a_citations_count+"\t"+a_h_index_count+"\t"+a_p_index_count+"\t"+a_keyterm+"\t"+uniqueSet.size()+"\t"+singleaut+"\t"+uniqueSet_venue.size()+"\t"+publication_year.get(0)+"\t"+publication_year.get(publication_year.size()-1)+"\n");
             String temp="";
             int totrow=0;

             uniqueSet.clear();
             publication_year.clear();
             publication_venue.clear();


          System.out.println("Compleate for =>"+i1+"\trecord no =>"+j1);
        //  System.out.println();

          System.gc();
	      }
	      conn.close();
	      writer.close();
	    //  System.out.println("Compleate for =>"+i1);

		  } catch (Exception e) {
		  e.printStackTrace();
		  }
	}

}
