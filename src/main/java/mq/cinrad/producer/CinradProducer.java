package mq.cinrad.producer;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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

	public static String CONFIG_ENDING = "cinrad.properties";
	public static String WATCH_DIR = "watch.dir";
	public static String DEFAULT_TOPIC = "default.topic";
	private static final Logger logger = LoggerFactory.getLogger(CinradProducer.class);

	public static void main(String[] args) {
		SpringApplication.run(CinradProducer.class, args);
	}

	@Override
	public void run(String... arg0) throws Exception {
		// TODO Auto-generated method stub
		if (null == arg0 || arg0.length == 0 || !arg0[0].endsWith(CONFIG_ENDING)) {
			logger.error("请在第一个参数指定cinrad.properties的完整路径");
			return;
		}

		logger.info(arg0[0]);
		Properties props = new Properties();
		FileInputStream fis = new FileInputStream(arg0[0]);
		props.load(fis);

		if (props.containsKey(WATCH_DIR) && props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
				&& props.containsKey(DEFAULT_TOPIC)) {

			// 创建单向队列queue DirWatch进程将监控到的文件放到队列中 Processor检查处理队列中的文件
			BlockingQueue<String> queue = new LinkedBlockingQueue<String>();

			DirWatch watch = new DirWatch(queue, props.getProperty(WATCH_DIR));
			Thread watchThread = new Thread(watch);
			watchThread.start();

			KafkaTemplate<String, byte[]> template = createTemplate(props);
			logger.info(props.getProperty(DEFAULT_TOPIC));
			template.setDefaultTopic(props.getProperty(DEFAULT_TOPIC));

			Producer processor = new Producer(queue, template);
			processor.run();
			// Thread processorThread = new Thread(processor);
			// processorThread.start();
		} else {
			logger.error("配置文件cinrad.properties有误,请检查:watch.dir,default.topic,bootstrap.servers");
		}

	}

	private KafkaTemplate<String, byte[]> createTemplate(Properties props) {
		Map<String, Object> senderProps = producerProps(props);
		ProducerFactory<String, byte[]> pf = new DefaultKafkaProducerFactory<String, byte[]>(senderProps);
		KafkaTemplate<String, byte[]> template = new KafkaTemplate<>(pf);
		return template;
	}

	private Map<String, Object> producerProps(Properties p) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, p.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		return props;
	}

}
