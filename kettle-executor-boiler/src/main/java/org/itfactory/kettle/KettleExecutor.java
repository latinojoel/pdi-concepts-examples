package org.itfactory.kettle;

import java.util.UUID;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingBuffer;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.core.logging.log4j.Log4jLogging;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

/**
 * Created with IntelliJ IDEA. User: puls3 Date: 11/27/13 Time: 6:06 PM To
 * change this template use File | Settings | File Templates.
 */
public enum KettleExecutor {
	INSTANCE;

	KettleExecutor() {
		try {
			KettleEnvironment.init();
			LoggingBuffer loggingBuffer = KettleLogStore.getAppender();
			loggingBuffer.addLoggingEventListener(new Log4jLogging());
		} catch (KettleException e) {
			e.printStackTrace();
		}
	}

	public int executeTransformation(String transPath, LogLevel logLevel) {
		int errors = 0;

		try {
			// Initialize the transformation
			TransMeta transMeta = new TransMeta(transPath, (Repository) null);

			Trans trans = new Trans(transMeta);
			trans.setLogLevel(logLevel);
			trans.setContainerObjectId(UUID.randomUUID().toString());
			trans.prepareExecution(null);

			// start and wait until it finishes
			trans.startThreads();
			trans.waitUntilFinished();

			// Remove the logging records
			String logChannelId = trans.getLogChannelId();
			KettleLogStore.discardLines(logChannelId, false);
			KettleLogStore.discardLines(transMeta.getLogChannelId(), false);
			LoggingRegistry.getInstance().removeIncludingChildren(logChannelId);

			errors = trans.getErrors();
		} catch (KettleException e) {
			e.printStackTrace();
		}

		return errors;
	}

	public int executeJob(String jobPath, LogLevel logLevel) {
		int errors = 0;

		try {
			// Initialize the transformation
			JobMeta jobMeta = new JobMeta(jobPath, null);

			Job job = new Job(null, jobMeta);
			job.setLogLevel(logLevel);
			job.setContainerObjectId(UUID.randomUUID().toString());

			// start and wait until it finishes
			job.start();
			job.waitUntilFinished();

			// Remove the logging records
			String logChannelId = job.getLogChannelId();
			KettleLogStore.discardLines(logChannelId, false);
			KettleLogStore.discardLines(jobMeta.getLogChannelId(), false);
			LoggingRegistry.getInstance().removeIncludingChildren(logChannelId);

			errors = job.getErrors();
		} catch (KettleException e) {
			e.printStackTrace();
		}

		return errors;
	}
}
