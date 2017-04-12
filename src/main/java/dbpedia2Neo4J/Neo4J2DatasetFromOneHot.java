package dbpedia2Neo4J;
import static org.neo4j.driver.v1.Values.parameters;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import randomWalks.StepType;

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
	private final Map<String,Set<Integer>> randomWalks;
	private final Map<String, Integer> randomWalkIds;
	private int walkCounter=1;

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
		randomWalks=new HashMap<>();
		randomWalkIds=new HashMap<>();
		logger.info("...Done");
	}

	/**
	 * Create the dataset from onehot csv file 
	 * @param oneHotCsv The onehot csv file 
	 */
	public void create(File oneHotCsv){
		boolean test=false;
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
						logger.info("{} lines parsed to create bins from attributes.", csvReader.getLinesRead());
					}

					if(test && csvReader.getLinesRead()>10000){
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
					List<StepType> allowedSteps=new ArrayList<>();
//					allowedSteps.add(StepType.ATTRIBUTE_PRESENCE);
					allowedSteps.add(StepType.ATTRIBUTE_VALUE);
					Set<String> walks=neo4jRandomWalkGenerator.getWalks(id, allowedSteps, binner,1,5);
					if(!walks.isEmpty()){
						logger.debug("{} : {}", id, walks);
						Set<String> ws = walks;
						
						Set<Integer> oneHotWs=randomWalks.get(id);
						if(oneHotWs==null){
							oneHotWs=new HashSet<>();
							randomWalks.put(id, oneHotWs);
						}
						for(String w:ws){
							Integer walkId=randomWalkIds.get(w);
							if(walkId==null){
								walkId=walkCounter++;
								randomWalkIds.put(w, walkId);
							}
							oneHotWs.add(walkId);
						}

					}
					if(csvReader.getLinesRead()%1000==0){
						logger.info("{} lines parsed to random walk.", csvReader.getLinesRead());
					}
					if(test && csvReader.getLinesRead()>10000){
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
			String[] newHeader=new String[header.length];
			PrintWriter headersX=new PrintWriter(new File(oneHotCsv.getParentFile(),"headerX.csv"));
			PrintWriter headersY=new PrintWriter(new File(oneHotCsv.getParentFile(),"headerY.csv"));

			headersY.println("header, short");
			for(int i=0;i<newHeader.length;i++){
				newHeader[i]="class_"+i;
			}
			newHeader[0]="id";
			for(int i=0;i<newHeader.length;i++){
				headersY.println(header[i]+","+newHeader[i]);
				headersY.flush();
			}
			headersY.close();


			CSVWriter datasetYWriter=null;

			String[] oneHotWalksHeader= new String[walkCounter];
			String[] shortOneHotWalksHeader= new String[walkCounter];
			oneHotWalksHeader[0]="id";
			shortOneHotWalksHeader[0]="id";
			for(String oneHotWalk:randomWalkIds.keySet()){
				oneHotWalksHeader[randomWalkIds.get(oneHotWalk)]=oneHotWalk;
				shortOneHotWalksHeader[randomWalkIds.get(oneHotWalk)]="walk_"+(randomWalkIds.get(oneHotWalk));
			}
			headersX.println("header, short");
			for(int i=0;i<oneHotWalksHeader.length;i++){
				headersX.println(oneHotWalksHeader[i]+","+shortOneHotWalksHeader[i]);
				headersX.flush();
			}
			headersX.close();
			CSVWriter datasetXWriter=null;
			File folder = new File(oneHotCsv.getParentFile(), "dataset");
			if(!folder.exists())
				folder.mkdir();

			//Read each line to get the id & then get random walks & create a dataset 
			//file for the walks and a dataset file for the classes.
			String[] row=null;
			int batch=0;
			int batchSize=20000;
			while((row=csvReader.readNext())!=null){
				long linesRead = csvReader.getLinesRead();
				if(linesRead>(batch*batchSize)){
					batch++;

					if(datasetYWriter!=null)
						datasetYWriter.close();
					datasetYWriter=new CSVWriter(new FileWriter(new File(folder,"datasetY_" + batch +".csv")), ',', CSVWriter.NO_QUOTE_CHARACTER);
					datasetYWriter.writeNext(newHeader);
					datasetYWriter.flush();

					if(datasetXWriter!=null)
						datasetXWriter.close();
					datasetXWriter=new CSVWriter(new FileWriter(new File(folder,"datasetX_" + batch +".csv")), ',', CSVWriter.NO_QUOTE_CHARACTER);
					datasetXWriter.writeNext(shortOneHotWalksHeader);
					datasetXWriter.flush();
				}
				// Print progress
				String id=row[0];
				if(!randomWalks.containsKey(id))
					continue;
				String[] oneHotWalksRow= new String[walkCounter];
				Set<Integer> cols=randomWalks.get(id);
				oneHotWalksRow[0]=id;
				for(int j=1;j<walkCounter;j++){
					if(cols.contains(j))
						oneHotWalksRow[j]="1";
					else
						oneHotWalksRow[j]="0";
				}
				datasetXWriter.writeNext(oneHotWalksRow);
				datasetXWriter.flush();

				datasetYWriter.writeNext(row);
				datasetYWriter.flush();


				if(csvReader.getLinesRead()%1000==0){
					logger.info("{} lines parsed to create the final dataset.", csvReader.getLinesRead());
				}
				if(test && csvReader.getLinesRead()>10000){
					break;
				}
			}
			csvReader.close();
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
