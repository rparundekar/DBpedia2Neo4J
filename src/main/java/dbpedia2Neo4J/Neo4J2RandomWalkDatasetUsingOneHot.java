package dbpedia2Neo4J;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import randomWalks.Neo4JRandomWalkGenerator;
import randomWalks.RelationshipLoadChecker;
import randomWalks.StepType;

/**
 *  This is a generator of the data from Neo4J and OneHot file
 *  NOTE: Currently only tested on Oct 2016 files for infobox_properties_en.ttl
 * @author rparundekar
 */
public class Neo4J2RandomWalkDatasetUsingOneHot{
	private static final int TEST_LINES = 3100;
	// SLF4J Logger bound to Log4J 
	private static final Logger logger=LoggerFactory.getLogger(Neo4J2RandomWalkDatasetUsingOneHot.class);
	// Driver object created once for the connection
	private final Driver driver;
	private final Map<String,Map<String,int[]>> allRandomWalks;
	private final Map<String,Map<String,Integer>> allRandomWalkIds;
	private final Map<String,Integer> walkCounters; 

	// Random Walks Generator
	private final Neo4JRandomWalkGenerator neo4jRandomWalkGenerator;
	private RelationshipLoadChecker binner;
	/**
	 * Create a new connection object to Neo4J, to the existing database
	 * @param neo4jUsername Username for Neo4J
	 * @param neo4jPassword Password for Neo4J
	 * @param deleteAll Should all existing nodes and edges be deleted?
	 */
	public Neo4J2RandomWalkDatasetUsingOneHot(String neo4jUsername, String neo4jPassword){
		logger.info("Connecting to Neo4J...");
		// Connect to Neo4J
		driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( neo4jUsername, neo4jPassword) );
		//Create the random walk generator
		neo4jRandomWalkGenerator=new Neo4JRandomWalkGenerator();
		allRandomWalks=new HashMap<>();
		allRandomWalkIds=new HashMap<>();
		walkCounters=new HashMap<>();
		logger.info("...Done");
	}

	/**
	 * Create the dataset from onehot csv file 
	 * @param oneHotCsv The onehot csv file 
	 * @param dataset 
	 */
	public void create(File oneHotCsv, String datasetName){
		binner=new RelationshipLoadChecker(oneHotCsv.getParentFile());
		boolean test=false;
		boolean createRelationshipBins = true;
		if(createRelationshipBins){
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
					long start=System.currentTimeMillis();
					while((row=csvReader.readNext())!=null){
						try(Session session=driver.session()){
							// Print progress
							String id=row[0];
							String query = "MATCH (t:Thing {id:'"+ id +"'})-[r]->(o:Thing) return t,r,o";
							StatementResult result = session.run(query);
							if(result.hasNext()){
								logger.debug("\t{} Found!", id);
								Record record = result.next();
								Relationship relationship = record.get("r").asRelationship();
								Node otherNode=record.get("o").asNode();
								binner.bin(relationship.type(), otherNode.get("id").asObject().toString());
							}
							if(csvReader.getLinesRead()%1000==0){
								logger.info("{} lines parsed to create bins from relationships in {} ms.", csvReader.getLinesRead(), (System.currentTimeMillis()-start));
								start = System.currentTimeMillis();
							}
							if(test && csvReader.getLinesRead()>TEST_LINES){
								break;
							}
						}catch (ClientException e) {
							e.printStackTrace();
							logger.error("Error in getting walks: {}",  e.getMessage());
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
		//Build the bins and write to file
		binner.buildBins();

		//Then, we create the dataset using random walks.
		try(Session session=driver.session()){
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
						// Print progress
						String id=row[0];
						List<StepType> allowedSteps=new ArrayList<>();
						allowedSteps.add(StepType.HAS_ATTRIBUTE);
						allowedSteps.add(StepType.HAS_RELATIONSHIP);
						allowedSteps.add(StepType.HAS_INCOMING_RELATIONSHIP);
						allowedSteps.add(StepType.RELATIONSHIP_STEP);
						//						allowedSteps.add(StepType.INCOMING_RELATIONSHIP_STEP);


						List<Integer> maxLengths = new ArrayList<>();
						maxLengths.add(2);

						//						maxLengths.add(5);
						//						maxLengths.add(7);

						List<Integer> numbersOfWalks = new ArrayList<>();
						numbersOfWalks.add(10);
						numbersOfWalks.add(25);
						numbersOfWalks.add(50);
						//						numbersOfWalks.add(10);
						//						numbersOfWalks.add(15);

						Map<String, Set<String>> allWalks=neo4jRandomWalkGenerator.getWalks(session, id, allowedSteps, binner,maxLengths, numbersOfWalks);
						//						Map<String, Set<String>> allWalks=neo4jRandomWalkGenerator.getAll(session, id, allowedSteps, binner);
						for(String dataset:allWalks.keySet()){
							Set<String> walks=allWalks.get(dataset);
							if(!walks.isEmpty()){
								logger.debug("{} : {}", id, walks.size());
								Set<String> ws = walks;
								Map<String, int[]> randomWalks = allRandomWalks.get(dataset);
								if(randomWalks==null)
								{
									randomWalks=new HashMap<>();
									allRandomWalks.put(dataset, randomWalks);
								}
								Map<String, Integer> randomWalkIds = allRandomWalkIds.get(dataset);
								if(randomWalkIds==null)
								{
									randomWalkIds=new HashMap<>();
									allRandomWalkIds.put(dataset, randomWalkIds);
								}
								if(!walkCounters.containsKey(dataset))
									walkCounters.put(dataset, 1);

								int walkCounter=walkCounters.get(dataset);

								int[] oneHotWs=randomWalks.get(id);
								if(oneHotWs==null){
									oneHotWs=new int[ws.size()];
									for(int i=0;i<oneHotWs.length;i++){
										oneHotWs[i]=-1;
									}
									randomWalks.put(id, oneHotWs);
								}
								int counter=0;
								for(String w:ws){
									Integer walkId=randomWalkIds.get(w);
									if(walkId==null){
										walkId=walkCounter++;
										randomWalkIds.put(w, walkId);
									}
									if(oneHotWs==null)
										System.err.println("Here");
									oneHotWs[counter]=walkId;
									counter++;
								}

								walkCounters.put(dataset, walkCounter);
							}
						}
						if(csvReader.getLinesRead()%1000==0){
							logger.info("{} lines parsed to random walk in {} ms.", csvReader.getLinesRead(), (System.currentTimeMillis()-start));
							start = System.currentTimeMillis();
						}
						if(test && csvReader.getLinesRead()>TEST_LINES){
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
		}catch (ClientException e) {
			e.printStackTrace();
			logger.error("Error in getting walks: {}",  e.getMessage());
		}

		for(String dataset:allRandomWalks.keySet()){
			try{
				File datasetFolder=new File(oneHotCsv.getParentFile(),datasetName+"_"+dataset);
				datasetFolder.mkdir();

				CSVReader csvReader = new CSVReader(new FileReader(oneHotCsv));
				//Read the header
				String[] header=csvReader.readNext();
				String[] newHeader=new String[header.length];
				PrintWriter headersX=new PrintWriter(new File(datasetFolder,"headerX.csv"));
				PrintWriter headersY=new PrintWriter(new File(datasetFolder,"headerY.csv"));

				headersY.println("header, short");
				for(int i=0;i<newHeader.length;i++){
					newHeader[i]="c"+i;
				}
				newHeader[0]="id";
				for(int i=0;i<newHeader.length;i++){
					headersY.println(header[i]+","+newHeader[i]);
					headersY.flush();
				}
				headersY.close();


				CSVWriter datasetYWriter=null;
				int walkCounter=walkCounters.get(dataset);
				Map<String, int[]> randomWalks = allRandomWalks.get(dataset);
				Map<String, Integer> randomWalkIds = allRandomWalkIds.get(dataset);

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

				PrintWriter sparseDataXWriter=null;
				File folder = new File(datasetFolder, "dataset");
				if(!folder.exists())
					folder.mkdir();

				//Read each line to get the id & then get random walks & create a dataset 
				//file for the walks and a dataset file for the classes.
				String[] row=null;
				int batch=0;
				int batchSize=5000;
				while((row=csvReader.readNext())!=null){
					long linesRead = csvReader.getLinesRead();
					if(linesRead>(batch*batchSize)){
						batch++;
						if(datasetYWriter!=null)
							datasetYWriter.close();
						datasetYWriter=new CSVWriter(new FileWriter(new File(folder,"datasetY_" + batch +".csv")), ',', CSVWriter.NO_QUOTE_CHARACTER);
						datasetYWriter.writeNext(newHeader);
						datasetYWriter.flush();

						if(sparseDataXWriter!=null)
							sparseDataXWriter.close();
						sparseDataXWriter=new PrintWriter(new FileWriter(new File(folder,"datasetX_" + batch +".csv")));
						String head=Arrays.toString(shortOneHotWalksHeader);
						head=head.substring(1,head.length()-1).trim();
						sparseDataXWriter.println(head);
						sparseDataXWriter.flush();
					}
					// Print progress
					String id=row[0];
					if(!randomWalks.containsKey(id))
						continue;
					int[] walks=randomWalks.get(id);
					Set<Integer> set = new HashSet<>();
					for(int k=0;k<walks.length;k++)
					{
						if(walks[k]==-1)
							continue;
						set.add(walks[k]);
					}

					String r = set.toString();
					r=r.substring(1, r.length()-1).trim();
					sparseDataXWriter.println(id + "," + r);
					sparseDataXWriter.flush();

					datasetYWriter.writeNext(row);
					datasetYWriter.flush();


					if(csvReader.getLinesRead()%10000==0){
						logger.info("{} lines parsed to create the final dataset.", csvReader.getLinesRead());
					}
					if(test && csvReader.getLinesRead()>TEST_LINES){
						break;
					}
				}
				csvReader.close();
				datasetYWriter.close();
				sparseDataXWriter.close();
			}catch(IOException e){
				// Something went wrong with the files.
				logger.error("Cannot write data to dataset file due to file issue:" + e.getMessage());
			}
		}
	}


	/**
	 * Get stuff running.
	 * @param args Have the username, password, if DB should be cleared AND list of files to load here
	 */
	public static void main(String[] args){
		Neo4J2RandomWalkDatasetUsingOneHot loadFile = new Neo4J2RandomWalkDatasetUsingOneHot("neo4j", "icd");
		loadFile.create(new File("/Users/rparundekar/dataspace/dbpedia2016/yagoOneHot.csv"), "yagoAll4Final");
	}


}
