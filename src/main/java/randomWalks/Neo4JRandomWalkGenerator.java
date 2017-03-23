package randomWalks;

import static org.neo4j.driver.v1.Values.parameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4JRandomWalkGenerator {
	// SLF4J Logger bound to Log4J 
	private static final Logger logger=LoggerFactory.getLogger(Neo4JRandomWalkGenerator.class);
	// Driver object created once for the connection
	private final Driver driver;
	private static final int DEFAULT_ATTRIBUTE_PRESENCE=5;
	private static final int DEFAULT_ATTRIBUTE_VALUES=5;

	public Neo4JRandomWalkGenerator(Driver driver){
		this.driver=driver;
	}

	public Map<RandomWalkExpressionType, Set<String>> getWalks(String id, Map<RandomWalkExpressionType, Integer> numberOfWalks, Binner binner) {
		Map<RandomWalkExpressionType, Set<String>> walks=new HashMap<>();
		String query = "MATCH (t:Thing) WHERE t.id = {id} return t";
		try(Session session=driver.session()){
			try (Transaction tx = session.beginTransaction())
			{
				StatementResult result = tx.run(query, parameters("id", id));
				if(result.hasNext()){
					logger.debug("\t{} Found!", id);

					//Find all attributes
					result = tx.run("MATCH (t:Thing) WHERE t.id = {id} return keys(t)", parameters("id", id));
					List<Object> attributes=new ArrayList<>();
					while(result.hasNext()){
						Record record = result.next();
						logger.debug("Attributes : {}",record.get("keys(t)").asList());
						attributes.addAll(record.get("keys(t)").asList());
					}
					//Remove "id" attribute
					attributes.remove("id");

					//Fetch number of walks to generate
					int numberOfWalksAttributePresence=DEFAULT_ATTRIBUTE_PRESENCE;
					if(numberOfWalks!=null && numberOfWalks.containsKey(RandomWalkExpressionType.ATTRIBUTE_PRESENCE) && numberOfWalks.get(RandomWalkExpressionType.ATTRIBUTE_PRESENCE)>=0)
						numberOfWalksAttributePresence=numberOfWalks.get(RandomWalkExpressionType.ATTRIBUTE_PRESENCE);
					int numberOfWalksAttributeValues=DEFAULT_ATTRIBUTE_VALUES;
					if(numberOfWalks!=null && numberOfWalks.containsKey(RandomWalkExpressionType.ATTRIBUTE_VALUES) && numberOfWalks.get(RandomWalkExpressionType.ATTRIBUTE_VALUES)>=0)
						numberOfWalksAttributeValues=numberOfWalks.get(RandomWalkExpressionType.ATTRIBUTE_VALUES);

					//Get random walks for attribute presence only
					if(!attributes.isEmpty()){

						//-----------------------Attribute Presence-----------------------//
						//Initialize variable to hold random walks for attribute presence
						Set<String> attributesPresent=new HashSet<>();
						//Perform random walks for attribute presence: Randomly pick numberOfWalksAttributePresence of the attributes;
						for(int i=0;i<numberOfWalksAttributePresence;i++)
						{
							String attribute=(String)attributes.get((int)(Math.random()*attributes.size()));
							if(!attributesPresent.contains(attribute))
								attributesPresent.add(attribute);
						}
						//Add the walks to the return variable
						walks.put(RandomWalkExpressionType.ATTRIBUTE_PRESENCE, attributesPresent);

						//-----------------------Attribute VALUES-----------------------//
						//Initialize variable to hold random walks for attribute presence
						Set<String> attributesValues=new HashSet<>();
						//Perform random walks for attribute presence: Randomly pick numberOfWalksAttributePresence of the attributes;
						for(int i=0;i<numberOfWalksAttributeValues;i++)
						{
							String attribute=(String)attributes.get((int)(Math.random()*attributes.size()));
							result = tx.run("MATCH (t:Thing) WHERE t.id = {id} return t." +attribute, parameters("id", id));
							while(result.hasNext()){
								Record record = result.next();
								Object values=record.get("t."+attribute).asObject();
								if(values instanceof List){
									//Get one random value;
									values=((List)values).get((int)(Math.random()*((List)values).size()));
								}
								String attributeValue=attribute + "=" + binner.getBin(attribute, values);
								if(!attributesValues.contains(attributeValue))
									attributesValues.add(attributeValue);
							}
						}
						//Add the walks to the return variable
						walks.put(RandomWalkExpressionType.ATTRIBUTE_VALUES, attributesValues);
						
					}
				}
				//WooHoo!
				tx.success();  
			}catch (ClientException e) {
				e.printStackTrace();
				logger.error("Error in getting walks: {}",  e.getMessage());
			}
		}
		return walks;
	}
}
