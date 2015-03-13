package HIVE_UDF;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class GET_URL  extends UDF {
	private static String result="";
	private static String inputLine="";
	
	public Text evaluate(final Text s) throws Exception{
	    if (s == null) { return null; }
	    String result = movie_num(s.toString());
	    return new Text("http://www.imdb.com/title/tt" + result+ "/");
	}
	  
	public static String movie_num(final String s) throws Exception{
		String url = "http://www.imdb.com/find?q=" +s.toString();
		URL website = new URL(url);
		Thread.sleep(500);//sleep to prevent block;
        URLConnection connection = website.openConnection();
        BufferedReader in = new BufferedReader(
                                new InputStreamReader(
                                    connection.getInputStream()));
        
        
        Matcher m=null;
        Pattern p = Pattern.compile("href=\"/title/tt([0-9]+)/.*");
        while ((inputLine = in.readLine()) != null) {
        	m = p.matcher(inputLine);
        	if(m.find()){
        		result = m.group(1);
        		break;
        	}
        }
        in.close();
        return result;
	}
}
