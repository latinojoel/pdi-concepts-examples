package org.itfactory.kettle;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.pentaho.di.core.logging.LogLevel;

/**
 * Created with IntelliJ IDEA. User: puls3 Date: 11/27/13 Time: 6:57 PM To
 * change this template use File | Settings | File Templates.
 */
public class KettleCallableExecutor implements Callable<Integer> {
	private static Logger logger = Logger.getLogger(KettleCallableExecutor.class);
	private static String KETTLE_JOB_SUFFIX = "kjb";
	private static String KETTLE_TRANS_SUFFIX = "ktr";

	private String path;
	private LogLevel logLevel;

	public KettleCallableExecutor(String path, LogLevel logLevel) {
		this.path = path;
		this.logLevel = logLevel;
	}

	public Integer call() throws Exception {
		if (this.path.endsWith(KETTLE_TRANS_SUFFIX)) {
			return KettleExecutor.INSTANCE.executeTransformation(this.path, this.logLevel);
		} else if (this.path.endsWith(KETTLE_JOB_SUFFIX)) {
			return KettleExecutor.INSTANCE.executeJob(this.path, this.logLevel);
		} else {
			return -1;
		}
	}
}
