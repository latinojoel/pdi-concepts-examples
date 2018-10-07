package org.itfactory.kettle;

import java.io.File;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.pentaho.di.core.logging.LogLevel;

/**
 * Hello world!
 *
 */
public class App {
	private static Logger logger = Logger.getLogger(App.class);
	private static String DEFAULT_TRANS_PATH = "kettle-gen-random-values.ktr";
	private static String DEFAULT_LOG_LEVEL = "Error";
	private static int DEFAULT_POOL_SIZE = 1;
	private static int DEFAULT_THREAD_NUM = 1;

	public static void main(String[] args) throws URISyntaxException {
		String path = new File(App.class.getClassLoader().getResource(DEFAULT_TRANS_PATH).toURI()).getPath();
		String logLevel = DEFAULT_LOG_LEVEL;
		int threadPoolSize = DEFAULT_POOL_SIZE;
		int threadNum = DEFAULT_THREAD_NUM;

		if (args.length > 0) {
			path = args[0];
			threadPoolSize = Integer.parseInt(args[1]);
			threadNum = Integer.parseInt(args[2]);
			logLevel = args[3];
		}

		ExecutorService pool = Executors.newFixedThreadPool(threadPoolSize);
		KettleCallableExecutor kce = new KettleCallableExecutor(path, LogLevel.getLogLevelForCode(logLevel));

		for (int i = 0; i < threadNum; i++) {
			pool.submit(kce);
		}

		pool.shutdown();
	}
}
