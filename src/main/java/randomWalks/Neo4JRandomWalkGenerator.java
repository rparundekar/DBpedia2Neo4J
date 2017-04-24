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

	public Map<String, Set<String>> getWalks(Session session, String id, List<StepType> allowedTypes, RelationshipLoadChecker binner, List<Integer> maxLengths, List<Integer> numbersOfWalks) {

		Map<String, List<String>> stepsCache = new HashMap<>();
		Map<String, List<String>> nextNodeCache = new HashMap<>();
		Map<String, Set<String>> allWalks = new HashMap<>();
		for(Integer maxLength:maxLengths){
			for(Integer numberOfWalks:numbersOfWalks){
				Set<String> walks=new HashSet<>();
				List<Integer> lengthList = new ArrayList<>();
				for(int i=1;i<=maxLength;i++)
					for(int j=maxLength;j>=i;j--)
						lengthList.add(i);
				for(int eachWalk=0;eachWalk<numberOfWalks;eachWalk++){
					int lengthOfWalk = lengthList.get((int)Math.floor(Math.random()*lengthList.size()));
					String currentNodeId=id;
					StringBuilder walk=new StringBuilder();
					for(int step=0;step<lengthOfWalk;step++){
						List<String> availableSteps = null;
						List<String> nextNodes = null;
						if(!stepsCache.containsKey(currentNodeId)){
							availableSteps = new ArrayList<String>();
							nextNodes = new ArrayList<String>();
							stepsCache.put(currentNodeId, availableSteps);
							nextNodeCache.put(currentNodeId, nextNodes);
							if(allowedTypes.contains(StepType.HAS_ATTRIBUTE)){
								Node node = null;
								String query = "MATCH (t:Thing {id:'"+ id +"'}) return t";
								StatementResult result = session.run(query);
								if(result.hasNext()){
									node = result.next().get("t").asNode();
								}
								if(node!=null){
									for(String attr:node.keys()){
										if(attr.equals("id"))
											continue;
										String s="has_" + attr+",";
										if(!availableSteps.contains(s)){
											availableSteps.add(s);
											nextNodes.add(currentNodeId);
										}
									}
								}
							}
							if((allowedTypes.contains(StepType.HAS_RELATIONSHIP)||allowedTypes.contains(StepType.RELATIONSHIP_STEP))){
								String query = "MATCH (t:Thing {id:'"+ id +"'})-[r]->(o:Thing) return t,r,o";
								StatementResult result = session.run(query);
								Node node=null;
								while(result.hasNext()){
									Record record = result.next();
									Relationship relationship = record.get("r").asRelationship();
									node = record.get("t").asNode();
									Node otherNode=record.get("o").asNode();
									String otherNodeId=otherNode.get("id").asObject().toString();
									if(allowedTypes.contains(StepType.HAS_RELATIONSHIP)){
										String s="hasRel_" + relationship.type() +",";
										if(!availableSteps.contains(s)){
											availableSteps.add(s);
											nextNodes.add(currentNodeId);
										}
									}
									if(allowedTypes.contains(StepType.RELATIONSHIP_STEP) && binner.canBin(relationship.type())){
										availableSteps.add(relationship.type() +"->");
										nextNodes.add(otherNodeId);
									}
								}
							}
							if((allowedTypes.contains(StepType.HAS_INCOMING_RELATIONSHIP)||allowedTypes.contains(StepType.INCOMING_RELATIONSHIP_STEP))){
								String query = "MATCH (o:Thing)-[r]->(t:Thing {id:'"+ id +"'}) return t,r,o";
								StatementResult result = session.run(query);
								Node node=null;
								while(result.hasNext()){
									Record record = result.next();
									Relationship relationship = record.get("r").asRelationship();
									node = record.get("t").asNode();

									Node otherNode=record.get("o").asNode();
									String otherNodeId=otherNode.get("id").asObject().toString();

									if(allowedTypes.contains(StepType.HAS_INCOMING_RELATIONSHIP)){
										String s="hasInRel_" + relationship.type()+",";
										if(!availableSteps.contains(s)){
											availableSteps.add(s);
											nextNodes.add(currentNodeId);
										}
									}
									if(allowedTypes.contains(StepType.INCOMING_RELATIONSHIP_STEP)){
										availableSteps.add(relationship.type() +"<-");
										nextNodes.add(otherNodeId);
									}
								}
							}
						}
						else{
							availableSteps=stepsCache.get(currentNodeId);
							nextNodes=nextNodeCache.get(currentNodeId);
						}
						if(availableSteps.isEmpty()){
							//Stay on same node
							continue;
						}
						int index=(int) Math.floor(Math.random()*availableSteps.size());
						String s=availableSteps.get(index);
						String nextNodeId = nextNodes.get(index);
						walk.append(s);
						currentNodeId=nextNodeId;
					}

					String w = walk.toString().trim();
					if(w.endsWith(","))
						w=w.substring(0, w.length()-1);
					else if(w.endsWith("->") || w.endsWith("<-"))
						w+="id="+currentNodeId;

					if(!walks.contains(w) && !w.isEmpty())
						walks.add(w);

				}
				allWalks.put(maxLength+ "x" + numberOfWalks, walks);
			}
		}
		return allWalks;
	}


	public Set<String> getAll(Session session, String id, List<StepType> allowedTypes, RelationshipLoadChecker binner) {
		Set<String> walks=new HashSet<>();

		String currentNodeId=id;
		List<String> availableSteps = null;
		availableSteps = new ArrayList<String>();
		if(allowedTypes.contains(StepType.HAS_ATTRIBUTE)){
			Node node = null;
			String query = "MATCH (t:Thing {id:'"+ id +"'}) return t";
			StatementResult result = session.run(query);
			if(result.hasNext()){
				node = result.next().get("t").asNode();
			}
			if(node!=null){
				for(String attr:node.keys()){
					if(attr.equals("id"))
						continue;
					String s="has_" + attr+",";
					if(!availableSteps.contains(s)){
						availableSteps.add(s);
					}
				}
			}
		}
		if((allowedTypes.contains(StepType.HAS_RELATIONSHIP))){
			String query = "MATCH (t:Thing {id:'"+ id +"'})-[r]->(o:Thing) return t,r,o";
			StatementResult result = session.run(query);
			while(result.hasNext()){
				Record record = result.next();
				Relationship relationship = record.get("r").asRelationship();
				String s="hasRel_" + relationship.type() +",";
				if(!availableSteps.contains(s)){
					availableSteps.add(s);
				}
			}
		}
		if((allowedTypes.contains(StepType.HAS_INCOMING_RELATIONSHIP))){
			String query = "MATCH (o:Thing)-[r]->(t:Thing {id:'"+ id +"'}) return t,r,o";
			StatementResult result = session.run(query);
			while(result.hasNext()){
				Record record = result.next();
				Relationship relationship = record.get("r").asRelationship();

				String s="hasInRel_" + relationship.type()+",";
				if(!availableSteps.contains(s)){
					availableSteps.add(s);
				}
			}
		}

		for(String walk:availableSteps){
			String w = walk.toString().trim();
			if(w.endsWith(","))
				w=w.substring(0, w.length()-1);
			else if(w.endsWith("->") || w.endsWith("<-"))
				w+="id="+currentNodeId;

			if(!walks.contains(w) && !w.isEmpty())
				walks.add(w);
		}
		return walks;
	}
}
