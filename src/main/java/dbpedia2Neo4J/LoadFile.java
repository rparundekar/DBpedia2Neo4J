package dbpedia2Neo4J;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

/**
 *  
 * @author rparundekar
 */
public class LoadFile {
	private final Driver driver;
	
	public LoadFile(String neo4jUsername, String neo4jPassword){
		driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( neo4jUsername, neo4jPassword) );
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
		loadFile.close();
	}
}
