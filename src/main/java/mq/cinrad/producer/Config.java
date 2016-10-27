package mq.cinrad.producer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Config {

	public static final String CONFIG_ENDING = "cinrad.properties";

	public static final String FILE_NAME_REGEX = "file.name.regex";
	public static final String FILE_NAME_REPLACEMENT = "file.name.replacement";

	public static final String WATCH_DIR = "watch.dir";
	public static final String DEFAULT_TOPIC = "default.topic";
	public static final String TOPIC_LIST = "topic.list";
	public static final String TOPIC_APPEND = "topic.append";

	public static final String[] CONFIG_ARRAY = { WATCH_DIR, DEFAULT_TOPIC, FILE_NAME_REGEX, FILE_NAME_REPLACEMENT };

	public static final Set<String> CONFIG_SET = new HashSet<>(Arrays.asList(CONFIG_ARRAY));

}
