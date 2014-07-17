package com.espn.nextgen.reporting.etl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.espn.nextgen.reporting.dao.ReportRepository;
import com.espn.nextgen.reporting.etl.manager.InputDataVisitor;
import com.espn.nextgen.reporting.exception.ETLProcessException;
import com.espn.nextgen.reporting.model.output.SubsetNewFansDocument;

/**
 * Hello world!
 *
 */
@Component
public class HiveQueries {
    @Autowired
    private ReportRepository reportRepo;

    private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final InputDataVisitor dataInput = InputDataVisitor.getInstance();

    private static final int MAX_RECORDS_IN_QUEUE = 100; // Maximum number of elements before discarding
    private static final int CORE_POOL_SIZE = 20;         // Start with this many threads in the pool
    private static final int MAX_THREAD_POOL_SIZE = 20;   // and have no more than this

    private ExecutorService executorService;
    private final LinkedBlockingQueue<Runnable> processingQueue = new LinkedBlockingQueue<Runnable>(MAX_RECORDS_IN_QUEUE);

    @PostConstruct
    void initProcessor() {
        executorService = new ThreadPoolExecutor(CORE_POOL_SIZE,
                                                 MAX_THREAD_POOL_SIZE,
                                                 5, TimeUnit.MINUTES, // After peak writes, kill idle threads after this amount of time
                                                 processingQueue,
                                                 new ReadHandlerThreadFactory(),           // Give our threads fancy names
                                                 new ThreadPoolExecutor.AbortPolicy()); // Make executor throw exception when unable to handle request

        ThreadPoolExecutor t = (ThreadPoolExecutor) executorService;
        t.allowCoreThreadTimeOut(true); // - When kill switch is on, we don't want idle threads hanging
    }

    private class ReadHandlerThreadFactory implements ThreadFactory{
        final AtomicInteger threadId = new AtomicInteger(0);
        final ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();

        @Override
        public Thread newThread(Runnable runnable) {
            String name = String.format("read-handler-thread-%d", threadId.getAndIncrement());
            Thread theThread = new Thread(threadGroup, runnable, name, 0);
            if (theThread.getPriority() != Thread.NORM_PRIORITY) {
                theThread.setPriority(Thread.NORM_PRIORITY);
            }
            if (theThread.isDaemon()) {
                theThread.setDaemon(false);
            }
            return theThread;
        }
    }

    public void executeQueries() throws SQLException {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }

//        Connection c = DriverManager.getConnection("jdbc:hive2://n7dmphgw.starwave.com:10000/default");
        Connection c = DriverManager.getConnection("jdbc:hive2://n7cldhutil01.dcloud.starwave.com:10000/default");
        Statement stmt = c.createStatement();
        stmt.execute("use espn_fuse_prod");
        stmt.execute("ADD JAR hdfs:///data/ESPN-FUSE-PROD/p13n-lib/json-serde-1.1.9.3.jar");

        SubsetNewFansDocument.executeQuery(executorService, reportRepo, 0);
    }

    public static void main(String[] args) throws SQLException, IOException, ETLProcessException {
        HiveQueries queries = new HiveQueries();
        queries.initProcessor();
        queries.executeQueries();

//        ActiveFans.fromResultSet(stmt, dataInput);

//        dataInput.persistReports(false);

        // // describe table
        // sql = "describe " + tableName;
        // System.out.println("Running: " + sql);
        // res = stmt.executeQuery(sql);
        // while (res.next()) {
        // System.out.println(res.getString(1) + "\t" + res.getString(2));
        // }
        //
        // // load data into table
        // // NOTE: filepath has to be local to the hive server
        // // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
        // String filepath = "/tmp/a.txt";
        // sql = "load data local inpath '" + filepath + "' into table " + tableName;
        // System.out.println("Running: " + sql);
        // res = stmt.executeQuery(sql);
        //
        // // select * query
        // sql = "select * from " + tableName;
        // System.out.println("Running: " + sql);
        // res = stmt.executeQuery(sql);
        // while (res.next()) {
        // System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
        // }
        //
        // // regular hive query
        // sql = "select count(1) from " + tableName;
        // System.out.println("Running: " + sql);
        // res = stmt.executeQuery(sql);
        // while (res.next()) {
        // System.out.println(res.getString(1));
        // }
    }
}
