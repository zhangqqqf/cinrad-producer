package mq.cinrad.producer;

import java.io.File;

/**
 * 文件操作的回调方法
 */
public interface FileActionCallback {

	public void delete(File file);

	public void modify(File file);

	public void create(File file);

}
