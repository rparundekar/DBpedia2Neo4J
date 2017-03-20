package dbpedia2Neo4J;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVWriter;

/**
 *  This is a loader for DBpedia type data into One Hot
 *  NOTE: Currently only tested on Oct 2016 files for instance_types_en.ttl
 * @author rparundekar
 */
public class DBpediaTypes2OneHot implements StreamRDF{
	// SLF4J Logger bound to Log4J 
	private static final Logger logger=LoggerFactory.getLogger(DBpediaTypes2OneHot.class);
	
	private final OntModel ontModel;
	
	private int oneHotCount = 0;
	private final Map<String, Integer> oneHotPosition;
	private final Map<String, Set<String>> types;
	/**
	 * Loads the Owl Ontology
	 * @param ontologyFile The OWL file
	 * @throws FileNotFoundException If file is not found
	 */
	public DBpediaTypes2OneHot(File ontologyFile){
		logger.info("Loading OWL file ...");
		OntModel base = ModelFactory.createOntologyModel( OntModelSpec.OWL_MEM );
		try {
			base.read( new FileInputStream(ontologyFile), "RDF/XML" );
		} catch (FileNotFoundException e) {
			logger.error("Could not find ontology file");
		}

		// create the reasoning model using the base
		ontModel = ModelFactory.createOntologyModel( OntModelSpec.OWL_MEM_MICRO_RULE_INF, base );
		oneHotPosition=new HashMap<>();
		types=new HashMap<>();
		logger.info("...Done");
	}

	/**
	 * Load the RDF data from the DBPedia turtle (.ttl) file 
	 * @param turtleFile The DBPedia turtle file e.g. instance_types.en 
	 */
	public void load(File turtleFile){
		try{
			
			// Step 1: Find the different types by iterating once through the instances
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
					logger.info("{} types present.", oneHotCount);
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
		try{	
			// Step 2: Create the csv with the onehot types;
			File outputCsv = new File(turtleFile.getParentFile(), "oneHot.csv");
			CSVWriter csvWriter = new CSVWriter(new FileWriter(outputCsv));
			//Write the header
			String[] header = new String[oneHotCount+1];
			header[0]="id";
			for(String id:oneHotPosition.keySet()){
				header[oneHotPosition.get(id)+1]=id;
			}
			csvWriter.writeNext(header);
			logger.info("Writing to oneHotFile... (Sit back & go grab a coffee. This may take a while.)");
			for(String subject:types.keySet()){
				Set<String> typeOf=types.get(subject);
				String[] row = new String[oneHotCount+1];
				row[0]=subject;
				for(String id:oneHotPosition.keySet()){
					if(typeOf.contains(id))
						row[oneHotPosition.get(id)+1]="1";
					else
						row[oneHotPosition.get(id)+1]="0";
				}
				csvWriter.writeNext(row);
			}
			csvWriter.close();
		}catch(IOException e){
			// Something went wrong with the files.
			logger.error("Cannot write the data to the file due to file issue:" + e.getMessage());
		}
	}
	
	/**
	 * Get stuff running.
	 * @param args Have the username, password, if DB should be cleared AND list of files to load here
	 */
	public static void main(String[] args){
		DBpediaTypes2OneHot loadFile = new DBpediaTypes2OneHot(new File("/Users/rparundekar/dataspace/dbpedia2016/dbpedia_2016-04.owl"));
		loadFile.load(new File("/Users/rparundekar/dataspace/dbpedia2016/instance_types_en.ttl"));
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
		subject=DBpediaHelper.stripClean(subject);

		// Get and clean the predicate URI (There are no blank nodes in DBpedia)
		String predicate = triple.getMatchPredicate().getURI();
		if(!predicate.equals(RDF.type.getURI())){
			logger.error("Property is not rdf:type");
		}

		// Get the object. It can be a URI or a literal (There are no blank nodes in the DBpedia)
		Node object = triple.getMatchObject();
		if(object.isURI()){
			// Get and clean the object URI (There are no blank nodes in DBpedia)
			String o=object.getURI();
			putTypes(subject,o);
			
			//System.out.println(subject + " " + o);
			OntClass ontClass=ontModel.getOntClass(o);
			oneHot(o);
			for (Iterator<OntClass> i = ontClass.listSuperClasses(true); i.hasNext(); ) {
				  OntClass c = i.next();
				  String type=c.getURI();
				  //System.out.println("\t" + c.getURI());
				  oneHot(type);
				  putTypes(subject,type);
			}
		}
		else{
			logger.error("Value of type is not a URI");
		}
		
	}

	private void putTypes(String subject, String type) {
		Set<String> typeOf = types.get(subject);
		if(typeOf==null)
		{
			typeOf=new HashSet<>();
			types.put(subject, typeOf);
		}
		if(!typeOf.contains(type))
			typeOf.add(type);
	}

	private void oneHot(String o) {
		if(!oneHotPosition.containsKey(o))
		{
			oneHotPosition.put(o, oneHotCount++);
		}
	}

	
}
