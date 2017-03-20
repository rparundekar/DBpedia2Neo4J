package dbpedia2Neo4J;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;

import randomWalks.Neo4JRandomWalkGenerator;
import randomWalks.RandomWalkExpressionType;

/**
 *  This is a generator of the data from Neo4J and OneHot file
 *  NOTE: Currently only tested on Oct 2016 files for infobox_properties_en.ttl
 * @author rparundekar
 */
public class Neo4J2DatasetFromOneHot{
	// SLF4J Logger bound to Log4J 
	private static final Logger logger=LoggerFactory.getLogger(Neo4J2DatasetFromOneHot.class);
	// Driver object created once for the connection
	private final Driver driver;
	
	private final RandomWalkExpressionType LEVEL=RandomWalkExpressionType.ATTRIBUTE_PRESENCE;
	// Random Walks Generator
	private final Neo4JRandomWalkGenerator neo4jRandomWalkGenerator;
	/**
	 * Create a new connection object to Neo4J, to the existing database
	 * @param neo4jUsername Username for Neo4J
	 * @param neo4jPassword Password for Neo4J
	 * @param deleteAll Should all existing nodes and edges be deleted?
	 */
	public Neo4J2DatasetFromOneHot(String neo4jUsername, String neo4jPassword){
		logger.info("Connecting to Neo4J...");
		// Connect to Neo4J
		driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( neo4jUsername, neo4jPassword) );
		//Create the random walk generator
		neo4jRandomWalkGenerator=new Neo4JRandomWalkGenerator(driver);
		logger.info("...Done");
	}

	/**
	 * Create the dataset from onehot csv file 
	 * @param oneHotCsv The onehot csv file 
	 */
	public void create(File oneHotCsv){
		try{
			CSVReader csvReader = new CSVReader(new FileReader(oneHotCsv));
			//Read the header
			String[] header=csvReader.readNext();
			if(!header[0].equals("id")){
				logger.error("First column is not the id");
			}else{
				//Read each line to get the id & then get random walks
				String[] row=null;
				while((row=csvReader.readNext())!=null){
					// Print progress
					String id=row[0];
					Map<RandomWalkExpressionType, Set<String>> walks=neo4jRandomWalkGenerator.getWalks(id, null);
					if(!walks.isEmpty())
						logger.info("{} : {}", id, walks);
					if(csvReader.getLinesRead()%1000==0){
						logger.info("{} lines parsed.", csvReader.getLinesRead());
					}
				}
			}
			// Close IO
			csvReader.close();
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
		Neo4J2DatasetFromOneHot loadFile = new Neo4J2DatasetFromOneHot("neo4j", "icd");
		loadFile.create(new File("/Users/rparundekar/dataspace/dbpedia2016/oneHot.csv"));
	}


}
