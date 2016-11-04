package mq.cinrad.kafka.producer;

import java.io.File;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class ProducerFileActionCallback implements FileActionCallback {

	private static final Logger logger = LoggerFactory.getLogger(ProducerFileActionCallback.class);

	private KafkaTemplate<String, String> template;

	private Properties props;

	private Set<String> topicSet;

	private String append;

	public ProducerFileActionCallback(KafkaTemplate<String, String> template, Properties prop) {

		this.template = template;
		this.props = prop;
		this.topicSet = new HashSet<>();
		if (this.props.containsKey(Config.TOPIC_LIST)) {
			String topicArray = this.props.getProperty(Config.TOPIC_LIST);
			if (null != topicArray && !topicArray.trim().equals("")) {
				if (topicArray.contains(",")) {
					String[] ta = topicArray.trim().split(",");
					for (String s : ta) {
						if (null != s && !s.equals("")) {
							this.topicSet.add(s);
						}
					}
				} else {
					this.topicSet.add(topicArray.trim());
				}

			}

		}

		this.append = this.props.getProperty(Config.TOPIC_APPEND, "-").trim();
	}

	@Override
	public void delete(File file) {

		if (null != file && null != file.getAbsolutePath()) {
			String fileUri = file.toURI().toString();
			logger.info("delete file: {}", fileUri);
		}

	}

	@Override
	public void modify(File file) {
		if (null != file && null != file.getAbsolutePath()) {
			String fileUri = file.toURI().toString();
			logger.info("modify file: {}", fileUri);
		}

	}

	@Override
	public void create(File file) {

		if (null != file && null != file.getAbsolutePath()) {
			// String fname = file.getAbsolutePath();
			String fileUri = file.toURI().toString();
			logger.info("create file: {}", fileUri);

			if (file.exists() && !file.isDirectory() && !file.getName().endsWith(".tmp")) {

				// byte[] data = Files.readAllBytes(f.toPath());
				// logger.info("data size: {}", data.length);
				// logger.info(props.getProperty(Config.FILE_NAME_REGEX));
				// logger.info(props.getProperty(Config.FILE_NAME_REPLACEMENT));
				// logger.info(file.toURI().toString());

				StringBuilder topicBuilder = new StringBuilder(template.getDefaultTopic()).append(this.append)
						.append(file.getParentFile().getName());

				String topic = template.getDefaultTopic();

				if (topicSet.contains(topicBuilder.toString())) {
					topic = topicBuilder.toString();
				}
				template.send(topic, fileUri, fileUri.replaceFirst(props.getProperty(Config.FILE_NAME_REGEX),
						props.getProperty(Config.FILE_NAME_REPLACEMENT)));
				template.flush();

			}

		}

	}

}
