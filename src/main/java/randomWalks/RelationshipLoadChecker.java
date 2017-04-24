package randomWalks;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelationshipLoadChecker {
	// SLF4J Logger bound to Log4J 
	private static final Logger logger=LoggerFactory.getLogger(RelationshipLoadChecker.class);
	private Map<String, Map<String, Integer>> values;
	private final Set<String> binRelationship;
	private File folder;
	public RelationshipLoadChecker(File folder){
		this.folder=folder;
		this.values=new HashMap<>();
		this.binRelationship=new HashSet<>();
	}

	public void bin(String relationship, String otherId) {
		Map<String, Integer> vs=values.get(relationship);
		if(vs==null){
			vs=new HashMap<>();
			values.put(relationship,vs);
		}
		if(!vs.containsKey(otherId))
			vs.put(otherId,1);
		else
			vs.put(otherId, vs.get(otherId)+1);
	}

	public void buildBins(){
		try{
			PrintWriter printWriter=new PrintWriter(new File(folder, "relationship.csv"));
			for(String relationship:values.keySet()){
				Map<String, Integer> valueCounts =  values.get(relationship);
				int uniqueCount=0;
				int countOfValues=0;
				for(Object o: valueCounts.keySet()){
					int count=valueCounts.get(o);
					countOfValues+=count;
					uniqueCount++;
				}
				double load = uniqueCount*1.0/countOfValues;
				printWriter.println(relationship +","+load+","+uniqueCount);
				printWriter.flush();
				if((load<0.2)){
					logger.info("Relationship {} has load {} and {} counts", relationship, load, uniqueCount);
					binRelationship.add(relationship);
				}else{
					//doNotBin
				}
			}
			printWriter.close();
			values=null;
			System.gc();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public String getBin(String relationship, String otherId){
		if(binRelationship.contains(relationship))
			return otherId;
		else
			return null;
	}


	public boolean canBin(String relationship) {
		return (this.binRelationship.contains(relationship));
	}
}
