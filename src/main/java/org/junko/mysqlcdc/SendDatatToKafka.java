package org.junko.mysqlcdc;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import scala.math.Numeric.IntIsIntegral;

/**
 * kafka 消息发送
 *
 */
public class SendDatatToKafka {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.12.141:6667");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer producer = null;
		try {

			producer = new KafkaProducer(props);
			for (int i = 0; i < 100; i++) {
				String msg = "Message " + i;
				producer.send(new ProducerRecord<String, String>("HelloKafkaTopic", msg));
				System.out.println("Sent:" + msg);
			}
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			producer.close();
		}

	}

	

	public void send(String topic, String key, String data) throws IOException {
		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.12.141:6667");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// key.serializer.class默认为serializer.class
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);
		for (int i = 0; i < 1000; i++) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			producer.send(new KeyedMessage<String, String>(topic, key, data + i));

		}

		producer.close();
	}
}