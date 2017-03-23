package dbpedia2Neo4J;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import static org.neo4j.driver.v1.Values.parameters;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import randomWalks.Binner;
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
	private final Map<String,Set<Integer>> oneHotWalks;
	private final Map<String, Integer> oneHotWalkIds;
	private int oneHotWalkIdCount=1;
	private final RandomWalkExpressionType LEVEL=RandomWalkExpressionType.ATTRIBUTE_PRESENCE;
	// Random Walks Generator
	private final Neo4JRandomWalkGenerator neo4jRandomWalkGenerator;
	private Binner binner;
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
		binner=new Binner();
		oneHotWalks=new HashMap<>();
		oneHotWalkIds=new HashMap<>();
		logger.info("...Done");
	}

	/**
	 * Create the dataset from onehot csv file 
	 * @param oneHotCsv The onehot csv file 
	 */
	public void create(File oneHotCsv){
		boolean test=true;
		//First, we load the attribute values for binning numbers, etc.
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
										if(key.equals("id"))
											continue;
										Object o=t.get(key).asObject();
										binner.bin(key, o);
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
					if(csvReader.getLinesRead()%1000==0){
						logger.info("{} lines parsed to create bins from properties.", csvReader.getLinesRead());
					}

					if(test && csvReader.getLinesRead()>100000){
						break;
					}
				}
			}
			// Close IO
			csvReader.close();
		}catch(IOException e){
			// Something went wrong with the files.
			logger.error("Cannot load the data from the file due to file issue:" + e.getMessage());
		}
		//Build the bins and write to file
		binner.buildBins();
		binner.writeToFile(oneHotCsv.getParentFile());
		//Then, we create the dataset using random walks.
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
					Map<RandomWalkExpressionType, Set<String>> walks=neo4jRandomWalkGenerator.getWalks(id, null, binner);
					if(!walks.isEmpty()){
						logger.debug("{} : {}", id, walks);
						for(RandomWalkExpressionType randomWalkExpressionType:walks.keySet()){
							if(randomWalkExpressionType!=RandomWalkExpressionType.ATTRIBUTE_PRESENCE){
								continue;
							}

							Set<String> ws = walks.get(randomWalkExpressionType);
							Set<Integer> oneHotWs=oneHotWalks.get(id);
							if(oneHotWs==null){
								oneHotWs=new HashSet<>();
								oneHotWalks.put(id, oneHotWs);
							}
							for(String w:ws){
								Integer walkId=oneHotWalkIds.get(w);
								if(walkId==null){
									walkId=oneHotWalkIdCount++;
									oneHotWalkIds.put(w, walkId);
								}
								oneHotWs.add(walkId);
							}
						}
					}
					if(csvReader.getLinesRead()%1000==0){
						logger.info("{} lines parsed to random walk.", csvReader.getLinesRead());
					}
					if(test && csvReader.getLinesRead()>100000){
						break;
					}
				}
			}
			// Close IO
			csvReader.close();
		}catch(IOException e){
			// Something went wrong with the files.
			logger.error("Cannot load the data from the file due to file issue:" + e.getMessage());
		}

		try{
			CSVReader csvReader = new CSVReader(new FileReader(oneHotCsv));
			//Read the header
			String[] header=csvReader.readNext();
			
			CSVWriter datasetYWriter=new CSVWriter(new FileWriter(new File(oneHotCsv.getParentFile(),"datasetY.csv")), ',', CSVWriter.NO_QUOTE_CHARACTER);
			datasetYWriter.writeNext(header);
			datasetYWriter.flush();
			
			CSVWriter datasetXWriter=new CSVWriter(new FileWriter(new File(oneHotCsv.getParentFile(),"datasetX.csv")), ',', CSVWriter.NO_QUOTE_CHARACTER);
			String[] oneHotWalksHeader= new String[oneHotWalkIdCount+1];
			oneHotWalksHeader[0]="id";
			for(String oneHotWalk:oneHotWalkIds.keySet()){
				oneHotWalksHeader[oneHotWalkIds.get(oneHotWalk)+1]=oneHotWalk;
			}
			datasetXWriter.writeNext(header);
			datasetXWriter.flush();
			
			//Read each line to get the id & then get random walks
			String[] row=null;
			while((row=csvReader.readNext())!=null){
				// Print progress
				String id=row[0];
				if(!oneHotWalks.containsKey(id))
					continue;
				String[] oneHotWalksRow= new String[oneHotWalkIdCount];
				Set<Integer> cols=oneHotWalks.get(id);
				oneHotWalksRow[0]=id;
				for(int j=1;j<oneHotWalkIdCount;j++){
					if(cols.contains(j))
						oneHotWalksRow[j]="true";
					else
						oneHotWalksRow[j]="false";
				}
				datasetXWriter.writeNext(oneHotWalksRow);
				datasetXWriter.flush();
				
				datasetYWriter.writeNext(row);
				datasetYWriter.flush();
				
				
				if(csvReader.getLinesRead()%1000==0){
					logger.info("{} lines parsed to create the final dataset.", csvReader.getLinesRead());
				}
				if(test && csvReader.getLinesRead()>100000){
					break;
				}
			}
			datasetYWriter.close();
			datasetXWriter.close();
		}catch(IOException e){
			// Something went wrong with the files.
			logger.error("Cannot write data to dataset file due to file issue:" + e.getMessage());
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
