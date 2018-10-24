/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrdc.solr2solr;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ludovic
 */
public class Starter {

    private final static Logger logger = LoggerFactory.getLogger(Starter.class);
    static final MetricRegistry metrics = new MetricRegistry();

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, IOException, SolrServerException, InterruptedException {
        String config = null;
        if (System.getProperty("config") != null) {
            config = System.getProperty("config");
        } else {
            logger.error("You must specify -Dconfig parameter!");
            System.exit(0);
        }

        Properties props = new Properties();
        props.load(new FileInputStream(config));

        //configure metrics
        Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(30, TimeUnit.SECONDS);

        String[] zkHostsSource = props.getProperty("source.zkHost").split(",");
        String[] zkHostsTarget = props.getProperty("target.zkHost").split(",");
        int nbThreads = Integer.valueOf(props.getProperty("nbThreads", "5"));
        String accountFiles = props.getProperty("queryLst.path");
        String sourceCollection = props.getProperty("source.collection");
        String targetCollection = props.getProperty("target.collection");
        //list of fields from the document
        String[] documentFields = props.getProperty("document.fields", "id").split(",");

        IndexClient sourceSolr = new IndexClient(zkHostsSource);
        IndexClient targetSolr = new IndexClient(zkHostsTarget);

        InputStream stream = new FileInputStream(accountFiles);
        List<String> accountList = IOUtils.readLines(stream, "UTF-8");

        ForkJoinPool threadPool = new ForkJoinPool(nbThreads);
        accountList.stream().forEach(query -> {
            threadPool.submit(new ProcessDocTask(sourceSolr, targetSolr, sourceCollection, targetCollection, query.trim(), documentFields));
        });

                
        Thread.sleep(3000);
        while (threadPool.hasQueuedSubmissions() || threadPool.getStealCount() > 0 || threadPool.getRunningThreadCount() > 0) {
            Thread.sleep(100);
        }
        threadPool.shutdown();
        System.exit(0);
    }

}
