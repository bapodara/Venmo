import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.xml.bind.DatatypeConverter;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class median_degree {

	/**
	 * @param args
	 */
	static ConcurrentSkipListMap<String, Long> runnigrecords = new ConcurrentSkipListMap<String,Long>();
	static TreeMap<String, Long> relationshipmap = new TreeMap< String, Long>();
    Long latestreading = 0L;
	JsonParser parser = new JsonParser();

	
	public static void main(String[] args) throws Exception {
		median_degree g = new median_degree();
		File fin = new File("venmo_input/venmo-trans.txt");
		File fout = new File("venmo_output/output.txt");
		FileReader fr = new FileReader(fin);
		
		FileWriter fw = new FileWriter(fout);
		String newLine = System.getProperty("line.separator"); 
		try(BufferedReader br = new BufferedReader(fr) ){
		    for(String line; (line = br.readLine()) != null; ) {
		    	
		    	if (g.validateLine(line)){
		    	
				g.addRecord(line);
				g.removeRecodrds();
				
				Map<String, Long> sortedMap = sortByValue(relationshipmap);				
				ArrayList<Long> a = new ArrayList<Long>(sortedMap.values());				
				if (a.size() == 2)
					fw.write(String.format("%.2f",(a.get(0)+ a.get(1) )/2.0)+newLine);				
				else if (a.size()%2 == 0)
					fw.write((String.format("%.2f",(a.get(a.size()/2)+ a.get(a.size()/2-1) )/2.0))+newLine);
				else
					fw.write(String.format("%.2f",a.get(a.size()/2)/1.0)+newLine);
		    	}
				
		    }
		    fr.close();
			fw.close();
		    
		}
	}

	private boolean validateLine(String Line)
	{
		boolean valid = false;
		String actor =null;
	    String target = null;
	    Long recordtime = null;
		try
		{
	    JsonObject jo = (JsonObject) parser.parse(Line);
	     actor = jo.get("actor").getAsString();
	     target = jo.get("target").getAsString();
	     recordtime =  getEpochTime(jo.get("created_time").getAsString());
		}catch(Exception e){}
		
	    if (null != actor  && null != target && null != recordtime)
	    {
	    	valid = true;
	    }
		
		return valid;
	}
	
	//Debug function to print maps
	private void printmap(Map m)
	{
		Set set = m.entrySet();
	      Iterator i = set.iterator();
	      while(i.hasNext()) {
	         Map.Entry me = (Map.Entry)i.next();
	         System.out.print(me.getKey() + ": ");
	         System.out.println(me.getValue());
	      }
	}
	
	private void addRecord(String row) throws Exception{
		
		
	    JsonObject jo = (JsonObject) parser.parse(row);
	    String actortarget = jo.get("actor").getAsString()+":"+jo.get("target").getAsString();
	    String targetactor = jo.get("target").getAsString()+":"+jo.get("actor").getAsString();
	    Long recordtime =  getEpochTime(jo.get("created_time").getAsString());
	    
	    if (recordtime>latestreading)
	    	latestreading = recordtime;
	    
	    if(!runnigrecords.containsValue(actortarget) && !runnigrecords.containsValue(targetactor)){
		    runnigrecords.put(actortarget, recordtime);
		    increaseRelationshipMap(actortarget);
	    }else{
	    	runnigrecords.values().removeAll(Collections.singleton(actortarget));
	    	runnigrecords.values().removeAll(Collections.singleton(targetactor));
	    	// no need to increase releationship as it must have been added from previous addition
	    	runnigrecords.put(actortarget, getEpochTime(jo.get("created_time").getAsString()));
	    }
	}
	
	private void removeRecodrds(){		
		for (String key : runnigrecords.keySet())
		{
			if(latestreading-runnigrecords.get(key)>59999){
				decreaseRelationshipMap(key);
				runnigrecords.remove(key);
			}
		}
		
	}
	
	
	private void decreaseRelationshipMap(String actortarget){
		String actor = actortarget.substring(0, actortarget.indexOf(":"));
		String target = actortarget.substring( actortarget.indexOf(":")+1);
		if (relationshipmap.get(actor)>1)
			relationshipmap.replace(actor, relationshipmap.get(actor)-1);
		else
			relationshipmap.remove(actor);
		if (relationshipmap.get(target)>1)
			relationshipmap.replace(target, relationshipmap.get(target)-1);
		else
			relationshipmap.remove(target);
	}
	
	
	
	
	private void increaseRelationshipMap(String actortarget){
		String actor = actortarget.substring(0, actortarget.indexOf(":"));
		String target = actortarget.substring( actortarget.indexOf(":")+1);
		if (null == relationshipmap.get(actor))
		{
			relationshipmap.put(actor, (long) 1);			
		}else
			relationshipmap.replace(actor, relationshipmap.get(actor)+1);
		if (null == relationshipmap.get(target))
		{
			relationshipmap.put(target, (long) 1);
		}else
			relationshipmap.replace(target, relationshipmap.get(target)+1);
	}
	
	
	
	private long getEpochTime(String time) throws Exception{
		final Calendar calendar2 = javax.xml.bind.DatatypeConverter.parseDateTime(time);
	    return (calendar2.getTimeInMillis()); 
	}
	
	
	private static Map<String, Long> sortByValue(TreeMap<String, Long> unsortMap) {
		// Convert Map to List
		List<Map.Entry<String, Long>> list = new LinkedList<Map.Entry<String, Long>>(unsortMap.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
			public int compare(Map.Entry<String, Long> o1,
                                           Map.Entry<String, Long> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		// Convert sorted map back to a Map
		Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
		for (Iterator<Map.Entry<String, Long>> it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Long> entry = it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

}
