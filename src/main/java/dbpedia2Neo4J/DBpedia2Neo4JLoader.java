package dbpedia2Neo4J;
import static org.neo4j.driver.v1.Values.parameters;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;

import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  This is a loader for DBpedia data as a Graph into Neo4J.
 *  NOTE: Currently only tested on Oct 2016 files for infobox_properties_en.ttl, instance_types_en.ttl
 * @author rparundekar
 */
public class DBpedia2Neo4JLoader implements StreamRDF{
	// SLF4J Logger bound to Log4J 
	private static final Logger logger=LoggerFactory.getLogger(DBpedia2Neo4JLoader.class);
	
	// Driver object created once for the connection
	private final Driver driver;
	
	/**
	 * Create a new connection object to Neo4J, to the existing database
	 * @param neo4jUsername Username for Neo4J
	 * @param neo4jPassword Password for Neo4J
	 * @param deleteAll Should all existing nodes and edges be deleted?
	 */
	public DBpedia2Neo4JLoader(String neo4jUsername, String neo4jPassword, boolean deleteAll){
		logger.info("Connecting to Neo4J, erasing if needed & creating index...");
		// Connect to Neo4J
		driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( neo4jUsername, neo4jPassword) );
		// Delete existing nodes and edges if that's what's needed
		if(deleteAll)
			deleteAll();
		//Create the default index
		createIndex();

		logger.info("...Done");
	}

	/**
	 * Load the RDF data from the DBPedia turtle (.ttl) file 
	 * @param turtleFile The DBPedia turtle file e.g. instance_types.en 
	 */
	public void load(File turtleFile){
		try{
			// Since the turtle file might contain errors (e.g. in the properties 
			// there is a value 'Infinity', with datatype xsd:double), we need to read each line
			// and then call the RDFDataMgr on that.
			// It sucks, since it's slow. But hey 'Infinity' cant be parsed as a double. 
			LineNumberReader lnr=new LineNumberReader(new FileReader(turtleFile));
			String line=null;
			while((line=lnr.readLine())!=null){
				// Print progress
				if(lnr.getLineNumber()%1000==0){
					logger.info("{} lines parsed.", lnr.getLineNumber());
				}
				// Parse the line read using the stream API. Make sure we catch parsing errors. 
				try{
					RDFDataMgr.parse(this, new StringReader(line), Lang.TURTLE);
				}catch(DatatypeFormatException de){
					logger.error("Illegal data format in line : " +line);
				}
			}
			// Close IO
			lnr.close();
		}catch(IOException e){
			// Something went wrong with the files.
			logger.error("Cannot load the data from the file due to file issue:" + e.getMessage());
		}
	}
	
	/**
	 * Close the driver to avoid memory leaks.
	 */
	public void close(){	
		driver.close();
	}
	
	/**
	 * Private function that cleans out the database. 
	 * Strong warning to safely use and pass right flag in the constructor.
	 */
	private void deleteAll(){
		Session session = driver.session();
		session.run( "MATCH (n) DETACH DELETE n");
		session.close();
	}
	
	/**
	 * Create default index on the identifier 
	 */
	private void createIndex(){
		Session session = driver.session();
		session.run( "CREATE INDEX ON :Thing(id)");
		session.close();
	}

	/**
	 * Get stuff running.
	 * @param args Have the username, password, if DB should be cleared AND list of files to load here
	 */
	public static void main(String[] args){
		DBpedia2Neo4JLoader loadFile = new DBpedia2Neo4JLoader("neo4j", "password", false);
		loadFile.load(new File("/Users/rparundekar/dataspace/dbpedia2016/infobox_properties_en.ttl"));
		loadFile.close();
	}

	@Override
	public void base(String base) {
		// Do Nothing
		// That's the base. DBpedia doesn't use this it seems.
	}

	@Override
	public void finish() {
		// Do Nothing
	}

	@Override
	public void prefix(String prefix, String iri) {
		// Do Nothing
	}

	@Override
	public void quad(Quad arg0) {
		// Do Nothing
	}

	@Override
	public void start() {
		// Do Nothing
	}

	@Override
	public void triple(Triple triple) {
		// Handle the triple
		
		// Get and clean the subject URI (There are no blank nodes in DBpedia)
		String subject = triple.getMatchSubject().getURI();
		subject=stripClean(subject);

		// Get and clean the predicate URI (There are no blank nodes in DBpedia)
		String predicate = triple.getMatchPredicate().getURI();
		predicate = stripClean(predicate);

		// Get the object. It can be a URI or a literal (There are no blank nodes in the DBpedia)
		Node object = triple.getMatchObject();
		Object o=null;
		if(object.isURI()){
			// Get and clean the object URI (There are no blank nodes in DBpedia)
			o=object.getURI();
			o=stripClean((String)o);
		}
		else{
			o=object.getLiteralValue();
			if(o instanceof String){
				// Just trim if it's a string
				o=o.toString().trim();
			}
			else if(o instanceof Number){
				// There may be some numbers passed as BigInteger which Neo4J cannot handle. 
				// So better to make all numbers double?
				o=((Number)o).doubleValue();
			}
			else if(object.getLiteralDatatypeURI().equals("http://www.w3.org/2001/XMLSchema#date")){
				// Handle date object
				String date=object.getLiteralLexicalForm();
				o=date; //Neo4J can't handle dates
			}else if(object.getLiteralDatatypeURI()!=null && object.getLiteralDatatypeURI().startsWith("http://dbpedia.org/datatype/")){
				// If there's a datatype, then clean it up.
				String v=object.getLiteralLexicalForm();
				o=Double.parseDouble(v);
				o=o + "__" + stripClean(object.getLiteralDatatypeURI());
			}
			else{
				// Else, just make it a string. 
				o=object.toString();
			}
		}

		try(Session session=driver.session()){
			try (Transaction tx = session.beginTransaction())
			{
				//Use MERGE to create/get the node for the subject in the graph
				StatementResult result = tx.run(
						"MERGE (s:Thing {id:{s}}) RETURN s",
						parameters("s", subject));
				Record record = result.next();
				org.neo4j.driver.v1.types.Node subjectNode=record.get("s").asNode();

				//If object is a URI, we need to create a relation
				if(object.isURI()){
					//The predicate is the relationship
					String relationship = predicate;
					
					//Use MERGE to create/get the node for the object in the graph
					result = tx.run(
							"MERGE (o:Thing {id:{o}}) RETURN o",
							parameters("o", o));
					record = result.next();
					org.neo4j.driver.v1.types.Node objectNode=record.get("o").asNode();
					
					//Create the link
					tx.run("MATCH (s:Thing),(o:Thing) WHERE id(s) = {s} AND id(o) = {o} CREATE (s)-[r:" + relationship + "]->(o)", parameters("s", subjectNode.id(), "o", objectNode.id()));
				}else{
					// The property is the predicate
					String property = predicate;
					
					// If the value had a datatype, append it to the property. This takes care of multiple values for the same property.
					if(o.toString().contains("__")){
						String split[] = o.toString().split("__");
						o=split[0];
						if(split.length>1)
							property+=  "__" + split[1].replaceAll("[^A-Za-z0-9]", "_");
					}
					// Set the property 
					tx.run("MATCH (s:Thing) WHERE id(s) = {s} SET s."+property+" = {o}", parameters("s", subjectNode.id(), "o", o));
				}
				//WooHoo!
				tx.success();  
			}catch (ClientException e) {
				logger.error("Error in inserting into database: {}",  e.getMessage());
			}
		}
		//If you really want to read stuff.
		logger.debug(subject + " : "+ predicate+ " : " + o);
	}

	/**
	 * Strips the DBpedia URI prefixes & replaces non alphanumeric characters to avoid property errors 
	 * @param uri The URI to strip & clean
	 * @return The last part of the URI if in DBpedia.
	 */
	private String stripClean(String uri) {
		if(uri.startsWith("http://dbpedia.org/resource/"))
			uri=uri.substring("http://dbpedia.org/resource/".length());
		if(uri.startsWith("http://dbpedia.org/property/"))
			uri=uri.substring("http://dbpedia.org/property/".length());
		if(uri.startsWith("http://dbpedia.org/datatype/"))
			uri=uri.substring("http://dbpedia.org/datatype/".length());
		// Note: This may cause some loss of information, but will prevent errors.
		return uri.replaceAll("[^A-Za-z0-9]", "_");
	}
}
