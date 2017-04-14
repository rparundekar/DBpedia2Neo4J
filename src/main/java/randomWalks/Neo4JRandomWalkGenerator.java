package randomWalks;

import static org.neo4j.driver.v1.Values.parameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4JRandomWalkGenerator {
	// SLF4J Logger bound to Log4J 
	private static final Logger logger=LoggerFactory.getLogger(Neo4JRandomWalkGenerator.class);


	public Set<String> getWalks(Session session, String id, List<StepType> allowedTypes, Binner binner, int maxLength, int numberOfWalks) {
		Set<String> walks=new HashSet<>();
		List<Integer> lengthList = new ArrayList<>();
		for(int i=1;i<=maxLength;i++)
			for(int j=maxLength;j>=i;j--)
				lengthList.add(i);

		Map<String, Node> nodeCache = new HashMap<>();

		for(int eachWalk=0;eachWalk<numberOfWalks;eachWalk++){
			int lengthOfWalk = lengthList.get((int)Math.floor(Math.random()*lengthList.size()));
			String currentNodeId=id;
			StringBuilder walk=new StringBuilder();
			for(int step=0;step<lengthOfWalk;step++){
				int index=(int) Math.floor(Math.random()*allowedTypes.size());
				StepType stepType = allowedTypes.get(index);
				switch(stepType){
				case HAS_ATTRIBUTE:
					//Find all attributes
					List<String> attributes=new ArrayList<>();
					Node node = null;
					if(nodeCache.containsKey(currentNodeId)){
						node=nodeCache.get(currentNodeId);
					}else{
						String query = "MATCH (t:Thing) WHERE t.id = {id} return t";
						StatementResult result = session.run(query, parameters("id", currentNodeId));
						if(result.hasNext()){
							node = result.next().get("t").asNode();
							if(!nodeCache.containsKey(currentNodeId)){
								nodeCache.put(currentNodeId, node);
							}
						}
					}
					if(node==null)
						continue;
					for(String attr:node.keys()){
						if(attr.equals("id"))
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
					node = null;
					if(nodeCache.containsKey(currentNodeId)){
						node=nodeCache.get(currentNodeId);
					}else{
						String query = "MATCH (t:Thing) WHERE t.id = {id} return t";
						StatementResult result = session.run(query, parameters("id", currentNodeId));
						if(result.hasNext()){
							node = result.next().get("t").asNode();
							if(!nodeCache.containsKey(currentNodeId)){
								nodeCache.put(currentNodeId, node);
							}
						}
					}
					if(node==null)
						continue;
					for(String attr:node.keys()){
						if(currentNodeId.equals(id) && attr.equals("id"))
							continue;
						Object value=node.get(attr).asObject();
						if(value instanceof List)
						{
							List<Object> list=node.get(attr).asList();
							value=(int)(Math.random()*list.size());
						}
						String bin=binner.getBin(attr, value);
						if(bin!=null)
							attrValues.add(attr +"="+bin);
//						else
//							attrValues.add("has_" + attr);
					}
					if(attrValues.size()==0){
						continue;
					}
					String attrVal=(String)attrValues.get((int)(Math.random()*attrValues.size()));
					walk.append(attrVal +",");
					//Stay on same node
					break;
				case HAS_RELATIONSHIP:
					//Find all relationships
					List<String> relationships=new ArrayList<>();
					String query = "MATCH (t:Thing)-[r]->() WHERE t.id = {id} return t,r";
					StatementResult result = session.run(query, parameters("id", currentNodeId));
					node=null;
					while(result.hasNext()){
						Record record = result.next();
						Relationship relationship = record.get("r").asRelationship();
						node = record.get("t").asNode();
						if(!nodeCache.containsKey(currentNodeId)){
							nodeCache.put(currentNodeId, node);
						}
						if(!relationships.contains(relationship.type()))
							relationships.add(relationship.type());
					}
					if(relationships.size()!=0){
						String relationship=(String)relationships.get((int)(Math.random()*relationships.size()));
						walk.append("hasRel_" + relationship +",");
					}
					//Stay on same node
					break;
				case HAS_INCOMING_RELATIONSHIP:
					//Find all relationships
					relationships=new ArrayList<>();
					query = "MATCH ()-[r]->(t:Thing) WHERE t.id = {id} return t,r";
					result = session.run(query, parameters("id", currentNodeId));
					node=null;
					while(result.hasNext()){
						Record record = result.next();
						Relationship relationship = record.get("r").asRelationship();
						node = record.get("t").asNode();
						if(!nodeCache.containsKey(currentNodeId)){
							nodeCache.put(currentNodeId, node);
						}
						if(!relationships.contains(relationship.type()))
							relationships.add(relationship.type());
					}
					if(relationships.size()!=0){
						String relationship=(String)relationships.get((int)(Math.random()*relationships.size()));
						walk.append("hasInRel_" + relationship +",");
					}
					//Stay on same node
					break;
				}

			}

			String w = walk.toString();
			if(w.endsWith(","))
				w=w.substring(0, w.length()-1);
			if(!walks.contains(w))
				walks.add(w);
		}
		return walks;
	}
}
