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
import java.util.Scanner;

public class partha_da
{
	
	public static void main(String[] args) throws FileNotFoundException, IOException
    {
		FileWriter writer = new FileWriter("F:/work/Aminer/22th_july/partha_da_mat.txt",true);
		BufferedReader s1 = new BufferedReader(new FileReader("F:/work/Aminer/22th_july/Detail_author_file_withHeading.csv"));
        ArrayList<String> list2 = new ArrayList<String>();
        String line="";
        String data[][]=new String[18250][8];
        int row=0;
        while ((line = s1.readLine()) != null){
            String lines[] = line.split(",");
        //    System.out.println(lines[0]);
            list2.add(lines[0]);
            data[row++][0]=lines[0];
            }
        
        int i,t;
        
        String st[]={
        			"authority_score.txt",
        			"betweenness_centrality.txt",
        			"closeness_centrality.txt",
        			"clustering_coefficient.txt",
        			"degree_centrality.txt",
        			"hub_score.txt",
        			"pagerank.txt"
        };
        
        for(int k=0;k<st.length;k++)
        	{
        	String fname=st[k];
        	Scanner s = new Scanner(new File("F:/work/Aminer/22th_july/"+fname));
            ArrayList<String> list = new ArrayList<String>();
            ArrayList<String> list1 = new ArrayList<String>();
            while (s.hasNext()){
                list.add(s.next());
                list1.add(s.next());
                }
            for(i=0;i<list2.size();i++)
                {
                    t=list.indexOf(list2.get(i));
                    data[i][k+1]=list1.get(t);
                }
            list.clear();
            list1.clear();
        	}
        	
        for(i=0;i<list2.size();i++)
            {
                writer.write(data[i][0]+"\t"+data[i][1]+"\t"+data[i][2]+"\t"+data[i][3]+"\t"+data[i][4]+"\t"+data[i][5]+"\t"+data[i][6]+"\t"+data[i][7]+"\n");
            }
        writer.close();
        System.out.println("Compleated.....");
	}

}
