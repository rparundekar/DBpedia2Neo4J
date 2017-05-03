package dbpedia2Neo4J;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;

/**
 *  This is a generator of the data from Neo4J and OneHot file
 *  NOTE: Currently only tested on Oct 2016 files for infobox_properties_en.ttl
 * @author rparundekar
 */
public class Neo4JCountsUsingOneHot{
	// SLF4J Logger bound to Log4J 
	private static final Logger logger=LoggerFactory.getLogger(Neo4JCountsUsingOneHot.class);
	// Driver object created once for the connection
	private final Driver driver;
	private int numberOfInstances=0;
	private int numberOfAttr=0;
	private int countAttr=0;
	private int numberOfRel=0;
	private int countRel=0;
	private int numberOfInRel=0;
	private int countInRel=0;
	private long numberOfAttrRelInRel=0;
	private int countAttrRelInRel=0;

	/**
	 * Create a new connection object to Neo4J, to the existing database
	 * @param neo4jUsername Username for Neo4J
	 * @param neo4jPassword Password for Neo4J
	 * @param deleteAll Should all existing nodes and edges be deleted?
	 */
	public Neo4JCountsUsingOneHot(String neo4jUsername, String neo4jPassword){
		logger.info("Connecting to Neo4J...");
		// Connect to Neo4J
		driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( neo4jUsername, neo4jPassword) );
		logger.info("...Done");
	}

	/**
	 * Create the dataset from onehot csv file 
	 * @param oneHotCsv The onehot csv file 
	 * @param dataset 
	 */
	public void count(File oneHotCsv){
		//First, we load the relationships for binning, etc.
		try{
			CSVReader csvReader = new CSVReader(new FileReader(oneHotCsv));
			//Read the header
			String[] header=csvReader.readNext();
			if(!header[0].equals("id")){
				logger.error("First column is not the id");
			}else{
				//Read each line to get the id & then get random walks
				String[] row=null;
				long start = System.currentTimeMillis();
				while((row=csvReader.readNext())!=null){
					try(Session session=driver.session()){
						// Print progress
						String id=row[0];
						String query = "MATCH (t:Thing {id:'"+ id +"'}) return t";
						StatementResult result = session.run(query);
						if(!result.hasNext()){
							continue;
						}
						logger.debug("\t{} Found!", id);
						numberOfInstances++;

						Set<String> thisSet=new HashSet<>();
						Record record = result.next();
						Node t=record.get("t").asNode();
						
						int c=0;
						for(String k:t.keys()){
							if(!thisSet.contains("has_" + k)){
								thisSet.add("has_" + k);
								numberOfAttr++;
							}
							c++;
						}
						if(c>0)
							countAttr++;

						query = "MATCH (t:Thing {id:'"+ id +"'})-[r]->(o:Thing) return t,r,o";
						result = session.run(query);
						c=0;
						while(result.hasNext()){
							record = result.next();
							Relationship relationship = record.get("r").asRelationship();
							String s="hasRel_" + relationship.type() +",";
							if(!thisSet.contains(s)){
								thisSet.add(s);
								numberOfRel++;
							}
							c++;
						}
						if(c>0)
							countRel++;
						
						query = "MATCH (o:Thing)-[r]->(t:Thing {id:'"+ id +"'}) return t,r,o";
						result = session.run(query);
						c=0;
						while(result.hasNext()){
							record = result.next();
							Relationship relationship = record.get("r").asRelationship();
							String s="hasInRel_" + relationship.type() +",";
							if(!thisSet.contains(s)){
								thisSet.add(s);
								numberOfInRel++;
							}
							c++;
						}
						if(c>0)
							countInRel++;
						
						numberOfAttrRelInRel+=thisSet.size();
						if(thisSet.size()>0)
							countAttrRelInRel++;
						
						if(csvReader.getLinesRead()%1000==0){
							logger.info("{} lines parsed to count in {} ms.", csvReader.getLinesRead(), (System.currentTimeMillis()-start));
							start = System.currentTimeMillis();
						}
					}catch (ClientException e) {
						e.printStackTrace();
						logger.error("Error in getting walks: {}",  e.getMessage());
					}
				}
			}
			// Close IO
			csvReader.close();

			logger.info("Number of instances: {}", numberOfInstances);
			logger.info("Average number of attributes: {}/{}={}", numberOfAttr, countAttr, (numberOfAttr*1.0/countAttr));
			logger.info("Average number of relationships: {}/{}={}", numberOfRel, countRel, (numberOfRel*1.0/countRel));
			logger.info("Average number of in relationships: {}/{}={}", numberOfInRel, countInRel, (numberOfInRel*1.0/countInRel));
			logger.info("Average number of all 3: {}/{}={}", numberOfAttrRelInRel, countAttrRelInRel, (numberOfAttrRelInRel*1.0/countAttrRelInRel));
		}catch(IOException e){
			// Something went wrong with the files.
			logger.error("Cannot load the data from the file due to file issue:" + e.getMessage());
		}
	}


	/**
	 * Get stuff running.
	 * @param args Have the username, password, if DB should be cleared AND list of files to load here
	 */
	public static void main(String[] args){
		Neo4JCountsUsingOneHot loadFile = new Neo4JCountsUsingOneHot("neo4j", "icd");
		loadFile.count(new File("/Users/rparundekar/dataspace/dbpedia2016/oneHot.csv"));
	}


}
