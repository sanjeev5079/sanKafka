package restProducerConsumer;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
public class restProducerPost {
	public static void main(String[] args) {

		try {

			Client client = Client.create();

			WebResource webResource = client.resource("http://192.168.28.133:8082/topics/testJSON1");
			//String recs = "\"records\":[{\"value\": {\"username\": \"testUser\"}},{\"value\": {\"username\": \"testUser2\"}}]";
			//String vschema = "\"value_schema\": \"{\"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"username\", \"type\": \"string\"}]}\"";
			//String input = "{"+vschema+","+recs+"}";
			String input = "{\"records\":[{\"value\":{\"username\":\"testUser\"}},{\"value\":{\"username\":\"testUser2\"}}]}";
			ClientResponse response = webResource.accept("application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json").type("application/vnd.kafka.json.v1+json").post(ClientResponse.class, input);
			/*ClientResponse response = webResource.accept("application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json")
	                .type("application/vnd.kafka.avro.v1+json").post(ClientResponse.class, input);*/
			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
				     + response.getStatus());
			}

			System.out.println("Output from Server .... \n");
			String output = response.getEntity(String.class);
			System.out.println(output);

		  } catch (Exception e) {

			e.printStackTrace();

		  }

		}
}
