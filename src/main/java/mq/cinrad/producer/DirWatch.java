package mq.cinrad.producer;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 *  
 * 功能说明：通过NIO 监控文件夹并放入队列中
 * </pre>
 */

public class DirWatch implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(DirWatch.class);

	private String sourceDir;
	private WatchService watcher;
	private Map<WatchKey, Path> keys;
	private FileActionCallback callback;

	public DirWatch(String sourceDir, FileActionCallback callback) {

		this.sourceDir = sourceDir;
		this.callback = callback;
	}

	@Override
	public void run() {

		keys = new HashMap<>();
		// 对文件夹进行监控
		try {
			watcher = FileSystems.getDefault().newWatchService();

			Path path = Paths.get(sourceDir);

			registerAll(path);

		} catch (IOException e) {
			logger.error("监控异常, 停止程序");

			System.exit(1);
		}

		processEvents(callback);

		/*
		 * while (true) { WatchKey key = watcher.take(); for (WatchEvent<?>
		 * event : key.pollEvents()) { // WatchEvent.Kind kind = event.kind();
		 * 
		 * if (event.kind() == OVERFLOW) { logger.warn("事件可能lost or discarded");
		 * continue; }
		 * 
		 * Path fileName = (Path) event.context();
		 * 
		 * logger.info(fileName.toString());
		 * logger.info(fileName.toAbsolutePath().toString());
		 * 
		 * //queue.add(fileName.toFile().getAbsolutePath());
		 * 
		 * } // 重置 key 如果失败结束监控 if (!key.reset()) {
		 * logger.error("重置Key失败，结束监控"); throw new
		 * RuntimeException("重置Key失败，结束监控"); // break; }
		 * 
		 * 
		 * }
		 */

	}

	/**
	 * 发生文件变化的回调函数
	 */
	@SuppressWarnings("rawtypes")
	void processEvents(FileActionCallback callback) {
		for (;;) {
			WatchKey key;
			try {
				key = watcher.take();
			} catch (InterruptedException x) {
				return;
			}
			Path dir = keys.get(key);
			if (dir == null) {
				logger.warn("操作未识别");
				continue;
			}

			for (WatchEvent<?> event : key.pollEvents()) {
				Kind kind = event.kind();

				// 事件可能丢失或遗弃
				if (kind == OVERFLOW) {
					continue;
				}

				// 目录内的变化可能是文件或者目录
				WatchEvent<Path> ev = cast(event);
				Path name = ev.context();
				Path child = dir.resolve(name);
				File file = child.toFile();
				if (kind.name().equals(FileAction.DELETE.getValue())) {
					callback.delete(file);
				} else if (kind.name().equals(FileAction.CREATE.getValue())) {
					callback.create(file);
				} else if (kind.name().equals(FileAction.MODIFY.getValue())) {
					callback.modify(file);
				} else {
					continue;
				}

				// if directory is created, and watching recursively, then
				// register it and its sub-directories
				if (kind == ENTRY_CREATE) {
					try {
						if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
							registerAll(child);
						}
					} catch (IOException x) {
						logger.error(x.getMessage());
					}
				}
			}

			boolean valid = key.reset();
			if (!valid) {
				// 移除不可访问的目录
				// 因为有可能目录被移除，就会无法访问
				keys.remove(key);
				logger.error("重置Key失败，{}目录被移除", key);
				// 如果待监控的目录都不存在了，就中断执行
				if (keys.isEmpty()) {
					logger.error("重置Key失败，监控的目录不存在,结束监控");
					break;
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	static <T> WatchEvent<T> cast(WatchEvent<?> event) {
		return (WatchEvent<T>) event;
	}

	/**
	 * 观察指定的目录，并且包括子目录
	 */
	private void registerAll(final Path start) throws IOException {
		Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
				register(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}

	/**
	 * 观察指定的目录
	 * 
	 * @param dir
	 * @throws IOException
	 */
	private void register(Path dir) throws IOException {
		WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
		keys.put(key, dir);
	}
}