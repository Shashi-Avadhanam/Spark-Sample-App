
package SparkMLApp.Activity;

import java.io.Serializable;
//import java.util.logging.Level;
import java.util.regex.*;

//import org.apache.log4j.Logger;
@SuppressWarnings("serial")
public class Activity  implements Serializable {
//private static final Logger logger = Logger.getLogger("Activity");

/**
	 * 
	 */
	//private static final long serialVersionUID = 1L;
private String user;
private String activity;
private String timestamp;
private double xaxis;
private double yaxis;
private double zaxis;

public String getUser() {
	return user;
}
public void setUser(String user) {
	this.user = user;
}
public String getActivity() {
	return activity;
}
public void setActivity(String activity) {
	this.activity = activity;
}
public String getTimestamp() {
	return timestamp;
}
public void setTimestamp(String timestamp) {
	this.timestamp = timestamp;
}
public double getXaxis() {
	return xaxis;
}
public void setXaxis(double xaxis) {
	this.xaxis = xaxis;
}
public double getYaxis() {
	return yaxis;
}
public void setYaxis(double yaxis) {
	this.yaxis = yaxis;
}
public double getZaxis() {
	return zaxis;
}
public void setZaxis(double zaxis) {
	this.zaxis = zaxis;
}

private Activity(String user, String activity, String timestamp, double xaxis,
		double yaxis, double zaxis) {
	this.user = user;
	this.activity = activity;
	this.timestamp = timestamp;
	this.xaxis = xaxis;
	this.yaxis = yaxis;
	this.zaxis = zaxis;
}

public Activity() {
	// TODO Auto-generated constructor stub
}
public static Activity parseFromLineRegex(String line) {
	
 String CSV_PATTERN ="(?:\\s*(?:\\\"([^\\\"]*)\\\"|([^,]+))\\s*,?)+?";
 Pattern PATTERN = Pattern.compile(CSV_PATTERN);
	
	   Matcher m = PATTERN.matcher(line);
	   if (!m.find()) {
	 //    logger.log(Level.ALL, "Cannot parse line" + line);
	     throw new RuntimeException("Error parsing line");
	   }
	   System.out.println("line: group1 group2 "+ line + m.group(1)+m.group(2)+m.group(3) );
	   return new Activity(m.group(1), m.group(2), m.group(3), Double.parseDouble(m.group(4)),
			   Double.parseDouble(m.group(5)), Double.parseDouble(m.group(6)));
	 }

public static Activity parseFromLine(String line) {
String spiltData []=line.split(",");
String user =spiltData[0];
 String activity =spiltData[1];
 String timestamp =spiltData[2];
 double xaxis =Double.parseDouble(spiltData[3]);
double yaxis =Double.parseDouble(spiltData[4]);
 double zaxis =Double.parseDouble(spiltData[5]);
return new Activity( user,  activity,  timestamp,  xaxis, yaxis,  zaxis);

}
	
}

