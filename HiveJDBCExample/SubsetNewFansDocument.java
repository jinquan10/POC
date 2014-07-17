package com.espn.nextgen.reporting.model.output;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Update;

import com.espn.nextgen.reporting.dao.ReportGeneratorMongoException;
import com.espn.nextgen.reporting.dao.ReportRepository;
import com.espn.nextgen.reporting.etl.lookup.LastRead;

@Document(collection = "dashboard")
public class SubsetNewFansDocument {
    private static final Logger logger = LoggerFactory.getLogger(SubsetNewFansDocument.class);

    @Transient
    private static final String hiveQuery = "SELECT COUNT(1) AS count FROM fan_fans WHERE fcreatedate > %s";

    @Transient
    public static final String REPORT_ID = "subsetNewFans";

    @Id
    private String id;
    private final String rptId = SubsetNewFansDocument.REPORT_ID;

    private Integer count;
    private LastRead lastRead;

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public LastRead getLastRead() {
        return lastRead;
    }

    public void setLastRead(LastRead lastRead) {
        this.lastRead = lastRead;
    }

    public SubsetNewFansDocument() {
    }

    public SubsetNewFansDocument(Integer count, LastRead lastRead) {
        this.count = count;
        this.lastRead = lastRead;
    }

    public Update getUpdate() {
        Update update = new Update();

        update.set("count", getCount());

        return update;
    }

    public static void executeQuery(ExecutorService executorService, final ReportRepository reportRepo, long bsonCutoffDate) {
        // TODO: Change this to represent the BSON file cutoff date
        final DateTime dateTime = new DateTime().withZone(DateTimeZone.UTC);

        for (final LastRead lastRead : LastRead.values()) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        String query = String.format(hiveQuery, dateTime.minusMonths(lastRead.getMonthsAgo()).getMillis() / 1000);
                        Connection c = DriverManager.getConnection("jdbc:hive2://n7cldhutil01.dcloud.starwave.com:10000/default");
                        Statement stmt = c.createStatement();
                        stmt.execute("use espn_fuse_prod");
                        stmt.execute("ADD JAR hdfs:///data/ESPN-FUSE-PROD/p13n-lib/json-serde-1.1.9.3.jar");
                        ResultSet result = stmt.executeQuery(query);

                        if (result.next()) {
                            Integer count = result.getInt("count");

                            reportRepo.writeSubsetNewFans(new SubsetNewFansDocument(count, lastRead));
                        }

                        result.close();
                        stmt.close();
                    } catch (SQLException e) {
                        logger.error("Could not write SubsetNewFans SQL", e);
                    } catch (ReportGeneratorMongoException e) {
                        logger.error("Could not write SubsetNewFans SQL", e);
                    }
                }
            });
        }
    }
}
