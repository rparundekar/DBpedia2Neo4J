package randomWalks;

import static org.neo4j.driver.v1.Values.parameters;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Binner {
	// SLF4J Logger bound to Log4J 
	private static final Logger logger=LoggerFactory.getLogger(Binner.class);
	// Driver object created once for the connection
	private final Driver driver;
	private final Map<String, Set<Object>> values;
	
	public Binner(Driver driver){
		this.driver=driver;
		this.values=new HashMap<>();
	}

	public void update(String id) {
		String query = "MATCH (t:Thing) WHERE t.id = {id} return t";
		try(Session session=driver.session()){
			try (Transaction tx = session.beginTransaction())
			{
				StatementResult result = tx.run(query, parameters("id", id));
				if(result.hasNext()){
					logger.debug("\t{} Found!", id);

					//Find all attributes
					result = tx.run("MATCH (t:Thing) WHERE t.id = {id} return t", parameters("id", id));
					while(result.hasNext()){
						Record record = result.next();
						Node t=record.get("t").asNode();
						Iterable<String> keys = t.keys();
						for(String key:keys){
							Set<Object> vs=values.get(key);
							if(vs==null){
								vs=new HashSet<>();
								values.put(key,vs);
							}
							Object o=t.get(key).asObject();
							if(!vs.contains(o))
								vs.add(o);
						}
					}
				}
				//WooHoo!
				tx.success();  
			}catch (ClientException e) {
				e.printStackTrace();
				logger.error("Error in getting walks: {}",  e.getMessage());
			}
		}
	}

	public void writeToFile(File parentFile) {
		File folder=new File(parentFile, "properties");
		if(!folder.exists())
			folder.mkdir();
		
		for(String property:values.keySet()){
			try {
				PrintWriter pw=new PrintWriter(new File(folder,property+".csv"));
				pw.println(property);
				Set<Object> vs=values.get(property);
				for(Object o:vs){
					pw.println(o.toString());
					pw.flush();
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
}
