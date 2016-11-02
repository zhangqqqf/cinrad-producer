package mq.cinrad.producer;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@SpringBootApplication
public class CinradProducer implements CommandLineRunner {

	private static final Logger logger = LoggerFactory.getLogger(CinradProducer.class);

	public static void main(String[] args) {
		SpringApplication.run(CinradProducer.class, args);
	}

	@Override
	public void run(String... arg0) throws Exception {
		
		if (null == arg0 || arg0.length == 0 || !arg0[0].endsWith(Config.CONFIG_ENDING)) {
			logger.error("请在第一个参数指定**cinrad.properties的完整路径");
			return;
		}

		logger.info(arg0[0]);
		Properties props = new Properties();
		FileInputStream fis = new FileInputStream(arg0[0]);
		props.load(fis);

		if (checkConfigFileValid(props)) {

			
			KafkaTemplate<String, String> template = createTemplate(props);
			//logger.info(props.getProperty(Config.DEFAULT_TOPIC));
			template.setDefaultTopic(props.getProperty(Config.DEFAULT_TOPIC));

			FileActionCallback callback = new ProducerFileActionCallback(template, props);
			DirWatch watch = new DirWatch(props.getProperty(Config.WATCH_DIR), callback);
			
			watch.run();

			
		} 

	}

	private boolean checkConfigFileValid(Properties props) {
        //logger.info(props.size()+"");
		for (String key : Config.CONFIG_SET) {
			if (!props.containsKey(key)) {
				logger.error("配置文件**cinrad.properties有误,请检查:{}", key);
				return false;
			}
		}

		return true;

	}

	private KafkaTemplate<String, String> createTemplate(Properties props) {
		Map<String, Object> senderProps = producerProps(props);
		ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<String, String>(senderProps);
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		return template;
	}

	private Map<String, Object> producerProps(Properties p) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, p.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 2048);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return props;
	}

}
