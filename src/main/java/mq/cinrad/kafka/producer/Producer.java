package mq.cinrad.kafka.producer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class Producer implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(Producer.class);

	private BlockingQueue<String> queue;
	private KafkaTemplate<String, byte[]> template;

	public Producer(BlockingQueue<String> queue, KafkaTemplate<String, byte[]> template) {
		this.queue = queue;
		this.template = template;
	}

	@Override
	public void run() {

		// KafkaTemplate<String, byte[]> template = createTemplate();
		try {
			while (true) {

				String fileName = queue.take();
				logger.info(fileName);

				if (null != fileName) {
					File f = new File(fileName);
					if (f.exists() && !f.isDirectory() && !f.getName().startsWith("ProductIndex")) {
						if (f.length() > 0) {
							byte[] data = Files.readAllBytes(f.toPath());
							logger.info("data size: {}", data.length);
							template.sendDefault(f.getName(), data);
							template.flush();
						}
					}
				}

			}
		} catch (InterruptedException e1) {

			// e.printStackTrace();
			logger.error(e1.getMessage());
		} catch (IOException e2) {

			logger.error(e2.getMessage());
		} finally {

			System.exit(1);
		}

	}

}
