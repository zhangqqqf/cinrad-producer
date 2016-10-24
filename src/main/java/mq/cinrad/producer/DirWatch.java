package mq.cinrad.producer;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

/**
 * <pre>
 *  
 * 功能说明：通过NIO 监控文件夹并放入队列中
 * </pre>
 */

public class DirWatch implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(DirWatch.class);

	private BlockingQueue<String> queue;
	private String sourceDir;

	public DirWatch(BlockingQueue<String> queue, String sourceDir) {
		this.queue = queue;
		this.sourceDir = sourceDir;
	}

	@Override
	public void run() {

		try {

			// 对文件夹进行监控
			Path path = Paths.get(sourceDir);
			WatchService watcher = FileSystems.getDefault().newWatchService();
			path.register(watcher, ENTRY_CREATE);

			while (true) {
				WatchKey key = watcher.take();
				for (WatchEvent<?> event : key.pollEvents()) {
					// WatchEvent.Kind kind = event.kind();

					if (event.kind() == OVERFLOW) {
						logger.warn("事件可能lost or discarded");
						continue;
					}

					Path fileName = (Path) event.context();

					logger.info(fileName.toString());
					logger.info(fileName.toAbsolutePath().toString());

					queue.add(fileName.toFile().getAbsolutePath());

				}
				// 重置 key 如果失败结束监控
				if (!key.reset()) {
					logger.error("重置Key失败，结束监控");
					throw new RuntimeException("重置Key失败，结束监控");
					// break;
				}
			}

		} catch (Exception e) {

			logger.error("监控异常, 停止程序");

			// 无法处理QAR数据 结束程序
			System.exit(1);
		}
	}
}