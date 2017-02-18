package dbpedia2Neo4J;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;

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
import org.neo4j.driver.v1.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  
 * @author rparundekar
 */
public class LoadFile implements StreamRDF{
	private static final Logger logger=LoggerFactory.getLogger(LoadFile.class);
	private final Driver driver;
	public LoadFile(String neo4jUsername, String neo4jPassword){
		driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( neo4jUsername, neo4jPassword) );
	}

	public void load(File file){
		try{
			LineNumberReader lnr=new LineNumberReader(new FileReader(file));
			String line=null;
			while((line=lnr.readLine())!=null){
				try{
					RDFDataMgr.parse(this, new StringReader(line), Lang.TURTLE) ;
				}catch(DatatypeFormatException de){
					logger.error("Illegal data format in line : " +line);
				}
			}
			lnr.close();
		}catch(IOException e){
			logger.error("Cannot load the data from the file due to file issue:" + e.getMessage());
		}
	}
	public void close(){	
		driver.close();
	}
	public void deleteAll(){
		Session session = driver.session();
		session.run( "MATCH (n) DETACH DELETE n");
		session.close();
	}

	public static void main(String[] args){
		LoadFile loadFile = new LoadFile("neo4j", "password");
		loadFile.load(new File("/Users/rparundekar/dataspace/dbpedia2016/infobox_properties_en.ttl"));
		loadFile.close();
	}

	public void base(String base) {
		logger.debug(base);
	}

	public void finish() {
		logger.debug("Done");
	}

	public void prefix(String prefix, String iri) {
		// Do Nothing
	}

	public void quad(Quad arg0) {
		// Do Nothing
	}

	public void start() {
		logger.debug("Starting to read the file");
	}

	public void triple(Triple triple) {
		String subject = triple.getMatchSubject().getURI();
		subject=strip(subject);
		String predicate = triple.getMatchPredicate().getURI();
		predicate = strip(predicate);
		Node object = triple.getMatchObject();
		Object o=null;
		if(object.isURI()){
			o=object.getURI();
			o=strip((String)o);
		}
		else{
			o=object.getLiteralValue();
			if(o instanceof Number || o instanceof String){
			}
			else if(object.getLiteralDatatypeURI().equals("http://www.w3.org/2001/XMLSchema#date")){
				String date=object.getLiteralLexicalForm();
				try {
					o=new SimpleDateFormat("yyyy-MM-dd").parse(date);
				} catch (ParseException e) {
					o=object.toString();
				}
			}else if(object.getLiteralDatatypeURI()!=null && object.getLiteralDatatypeURI().startsWith("http://dbpedia.org/datatype/")){
				String s=object.getLiteralLexicalForm();
				o=Double.parseDouble(s);
				o=o + "__" + object.getLiteralDatatypeURI().substring("object.getLiteralDatatypeURI()".length());
			}
			else{
				o=object.toString();
			}
		}
		logger.debug(subject + " : "+ predicate+ " : " + o);
	}

	private String strip(String uri) {
		if(uri.startsWith("http://dbpedia.org/resource/"))
			return uri.substring("http://dbpedia.org/resource/".length());
		if(uri.startsWith("http://dbpedia.org/property/"))
			return uri.substring("http://dbpedia.org/property/".length());
		//logger.warn("Error in stripping away: " + uri);
		return uri;
	}
}
