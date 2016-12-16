package cl.citiaps.neo4j.main;

import java.io.IOException;
import java.util.*;

import org.bson.Document;
import org.bson.BasicBSONObject;
import org.neo4j.driver.v1.*;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.BasicDBObject;



public class Main {
	
		public static void main(String[] args) throws IOException {
			@SuppressWarnings("resource")
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("pruebaFiltro");

			MongoCollection<Document> coleccion = db.getCollection("tweets4");
			
			
			Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "tbd2016" ) );
			Session sesion = driver.sesion();

			System.out.println(sesion.isOpen());

			procesar(coleccion,sesion);

			sesion.close();
			driver.close();

		}
		
		public void procesar(MongoCollection<Document> coleccion, Session sesion) throws IOException {

		sesion.run( String.format("MATCH (n) DETACH DELETE n") );

		for (Document doc : coleccion.find() ) {
			
			Document usuario = (Document)doc.get("user");
			if (doc.get("in_reply_to_screen_name") != null ) {						
			
				String replyId = "u_"+doc.getString("in_reply_to_screen_name");
				String idUsuario = "u_"+usuario.getString("screen_name");
				String existe = "0";
				StatementResult result = sesion.run( String.format("MATCH ( (%s:User{screen_name:\"%s\"})-[rep:REPLY{type:\"reply\"}]->(%s:User{screen_name:\"%s\"}) ) RETURN 1 AS existe", idUsuario, idUsuario, replyId, replyId) );
				if (result.hasNext()) {
					Record record =result.next();
					existe = record.get("existe").toString();
				}								


				if  (!existe.equals("1")) {
					if (idUsuario.equals(replyId)) { }

					else{
						sesion.run( String.format("MERGE (%s:User {screen_name:\"%s\"}) MERGE (%s:User {screen_name:\"%s\"}) MERGE (%s) - [rep:REPLY {type:\"reply\", count:1} ] -> (%s)", idUsuario, idUsuario, replyId, replyId, idUsuario, replyId) );
					}
				}
				else{
					if (idUsuario.equals(replyId)) { }

					else{
						sesion.run( String.format("MATCH (%s:User {screen_name:\"%s\"})-[rep:REPLY{type:\"reply\"}]->(%s:User {screen_name:\"%s\"}) SET rep.count = rep.count + 1", idUsuario, idUsuario, replyId, replyId) );
					}
				}

				

							
				
			}
			
			Document retweet = (Document)doc.get("retweeted_status");
			if (retweet!=null) {
				String idUsuario = "u_"+usuario.getString("screen_name");
				Document usuarioRetweeteado = (Document)retweet.get("user");
				String idUsuarioRetweet = "u_"+usuarioRetweeteado.getString("screen_name");
				String existe = "0";
				StatementResult result = sesion.run( String.format("MATCH ( (%s:User{screen_name:\"%s\"})-[retw:RETWEET{type:\"retweet\"}]->(%s:User{screen_name:\"%s\"}) ) RETURN 1 AS existe", idUsuario, idUsuario, idUsuarioRetweet, idUsuarioRetweet) );
				if (result.hasNext()) {
					Record record =result.next();
					existe = record.get("existe").toString();
				}
				
				if (!existe.equals("1")) { 
					if (idUsuario.equals(idUsuarioRetweet)) { }
					else{
						sesion.run( String.format("MERGE (%s:User {screen_name:\"%s\"}) MERGE (%s:User {screen_name:\"%s\"}) MERGE (%s) - [retw:RETWEET{type:\"retweet\", count:1} ] -> (%s)", idUsuario, idUsuario, idUsuarioRetweet, idUsuarioRetweet, idUsuario, idUsuarioRetweet) );
					}
				}
				else{
					if (idUsuario.equals(idUsuarioRetweet)) { }
					else{
						sesion.run( String.format("MATCH (%s:User {screen_name:\"%s\"})-[retw:RETWEET{type:\"retweet\"}]->(%s:User {screen_name:\"%s\"}) SET retw.count = retw.count + 1", idUsuario, idUsuario, idUsuarioRetweet, idUsuarioRetweet) );
					}
				}

			}

			
			Document entities = (Document)doc.get("entities");

			if (!entities.get("user_mentions").toString().equals("[]")) {
				String idUsuario = "u_"+usuario.getString("screen_name");
				sesion.run( String.format("MERGE (%s:User {screen_name:\"%s\"})", idUsuario, idUsuario) );
				String token, idMencionado;
				ArrayList<String> nombres = new ArrayList<String>();

				StringTokenizer st = new StringTokenizer(entities.get("user_mentions").toString()," ");
			    while (st.hasMoreTokens()) {
			    	token=st.nextToken();
			    	if (token.startsWith("screen_name") ) {
			    		nombres.add(token);
			    	}			    	
			    }
			    for (int i=0;i<nombres.size();i++ ) {
			    	String[] parts = nombres.get(i).split("=");
			    	idMencionado="u_"+parts[1].replace(",", "");	
			    	String existe = "0";
					StatementResult result = sesion.run( String.format("MATCH ( (%s:User{screen_name:\"%s\"})-[ment:MENTION{type:\"mention\"}]->(%s:User{screen_name:\"%s\"}) ) RETURN 1 AS existe", idUsuario, idUsuario, idMencionado, idMencionado) );
					if (result.hasNext()) {
						Record record =result.next();
						existe = record.get("existe").toString();
					}			    	
			    	
					if (!existe.equals("1")) { 
						if (idUsuario.equals(idMencionado)) { }
						else{
							sesion.run( String.format("MATCH (%s:User {screen_name:\"%s\"}) MERGE (%s:User {screen_name:\"%s\"}) MERGE (%s) - [ment:MENTION{type:\"mention\", count:1} ] -> (%s)", idUsuario, idUsuario, idMencionado, idMencionado, idUsuario, idMencionado) );
						}
					}
					else{
						if (idUsuario.equals(idMencionado)) {}
						else{
							sesion.run( String.format("MATCH (%s:User {screen_name:\"%s\"})-[ment:MENTION{type:\"mention\"}]->(%s:User {screen_name:\"%s\"}) SET ment.count = ment.count + 1", idUsuario, idUsuario, idMencionado, idMencionado) );
						}
					}

			    }
			    


			}
		}
	}
	

}
