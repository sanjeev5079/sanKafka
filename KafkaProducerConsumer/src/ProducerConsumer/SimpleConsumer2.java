package ProducerConsumer;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.FetchResponse;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;

import java.nio.ByteBuffer;
import java.util.*;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import java.io.UnsupportedEncodingException;

//public class SimpleConsumer2 {
	public class SimpleConsumer2 extends  Thread {
	    final static String clientId = "SimpleConsumerDemoClient";
	    final static String TOPIC = "test";
	    ConsumerConnector consumerConnector;


	    public static void main(String[] argv) throws UnsupportedEncodingException {
	    	SimpleConsumer2 helloKafkaConsumer = new SimpleConsumer2();
	        helloKafkaConsumer.start();

	    }

	    public SimpleConsumer2(){
	        Properties properties = new Properties();
	        properties.put("zookeeper.connect","localhost:2181");
	        properties.put("group.id","test-consumer-group");
	        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
	        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
	    }

	    @Override
	    public void run() {
	        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	        topicCountMap.put(TOPIC, new Integer(1));
	        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
	        KafkaStream<byte[], byte[]> stream =  consumerMap.get(TOPIC).get(0);
	        ConsumerIterator<byte[], byte[]> it = stream.iterator();
	        while(it.hasNext())
	            System.out.println(new String(it.next().message()));

	    }

	    private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
	        for(MessageAndOffset messageAndOffset: messageSet) {
	            ByteBuffer payload = messageAndOffset.message().payload();
	            byte[] bytes = new byte[payload.limit()];
	            payload.get(bytes);
	            System.out.println("output is"+new String(bytes, "UTF-8"));
	        }
	    
	}
}
