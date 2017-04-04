package randomWalks;

import static org.neo4j.driver.v1.Values.parameters;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4JRandomWalkGenerator {
	// SLF4J Logger bound to Log4J 
	private static final Logger logger=LoggerFactory.getLogger(Neo4JRandomWalkGenerator.class);
	// Driver object created once for the connection
	private final Driver driver;

	public Neo4JRandomWalkGenerator(Driver driver){
		this.driver=driver;
	}

	public Set<String> getWalks(String id, List<StepType> allowedTypes, Binner binner, int maxLength, int numberOfWalks) {
		Set<String> walks=new HashSet<>();
		
		for(int eachWalk=0;eachWalk<numberOfWalks;eachWalk++){
			int lengthOfWalk = (int) Math.ceil(Math.random()*maxLength);
			String query = "MATCH (t:Thing) WHERE t.id = {id} return t";
			try(Session session=driver.session()){
				try (Transaction tx = session.beginTransaction())
				{
					StatementResult result = tx.run(query, parameters("id", id));
					if(result.hasNext()){
						logger.debug("\t{} Found!", id);
						Node node = result.next().get("t").asNode();
						Node startNode=node;
						StringBuilder walk=new StringBuilder();
						for(int step=0;step<lengthOfWalk;step++){
							int index=(int) Math.floor(Math.random()*allowedTypes.size());
							StepType stepType = allowedTypes.get(index);
							switch(stepType){
							case ATTRIBUTE_PRESENCE:
								//Find all attributes
								List<String> attributes=new ArrayList<>();
								for(String attr:node.keys()){
									if(node.id()==startNode.id() && attr.equals("id"))
										continue;
									attributes.add(attr);
								}
								if(attributes.size()==0){
									continue;
								}
								String attribute=(String)attributes.get((int)(Math.random()*attributes.size()));
								walk.append("has_" + attribute +",");
								//Stay on same node
								break;
							case ATTRIBUTE_VALUE:
								//Find all attributes & values
								List<String> attrValues=new ArrayList<>();
								for(String attr:node.keys()){
									if(node.id()==startNode.id() && attr.equals("id"))
										continue;
									Object value=node.get(attr).asObject();
									if(value instanceof List)
									{
										List<Object> list=node.get(attr).asList();
										value=(int)(Math.random()*list.size());
									}
									attrValues.add(attr +"="+value.toString());
								}
								if(attrValues.size()==0){
									continue;
								}
								String attrVal=(String)attrValues.get((int)(Math.random()*attrValues.size()));
								walk.append(attrVal +",");
								//Stay on same node
								break;
							default:
								break;
							
							}
						}
						walks.add(walk.toString());
					}
					//WooHoo!
					tx.success();  
				}catch (ClientException e) {
					e.printStackTrace();
					logger.error("Error in getting walks: {}",  e.getMessage());
				}
			}
		}
		return walks;
	}
}
