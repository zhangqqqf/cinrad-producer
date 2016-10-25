package mq.cinrad.producer;

import java.io.File;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class ProducerFileActionCallback implements FileActionCallback {

	private static final Logger logger = LoggerFactory.getLogger(ProducerFileActionCallback.class);

	private KafkaTemplate<String, String> template;

	private Properties props;

	public ProducerFileActionCallback(KafkaTemplate<String, String> template, Properties props) {

		this.template = template;
		this.props = props;
	}

	@Override
	public void delete(File file) {

		if (null != file && null != file.getAbsolutePath()) {
			logger.info("delete file: {}", file.getAbsolutePath());
		}
	}

	@Override
	public void modify(File file) {
		if (null != file && null != file.getAbsolutePath()) {
			logger.info("modify file: {}", file.getAbsolutePath());
		}
	}

	@Override
	public void create(File file) {

		if (null != file && null != file.getAbsolutePath()) {
			//String fname = file.getAbsolutePath();

			logger.info("create file: {}", file.getAbsolutePath());

			if (file.exists() && !file.isDirectory()) {

				// byte[] data = Files.readAllBytes(f.toPath());
				// logger.info("data size: {}", data.length);
				//logger.info(props.getProperty(Config.FILE_NAME_REGEX));
				//logger.info(props.getProperty(Config.FILE_NAME_REPLACEMENT));
				
				logger.info(file.toURI().toString());
				
				
				template.sendDefault(file.getAbsolutePath(), file.toURI().toString().replaceFirst(
						props.getProperty(Config.FILE_NAME_REGEX), props.getProperty(Config.FILE_NAME_REPLACEMENT)));
				template.flush();

			}
		}

	}

}
