/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrdc.solr2solr;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
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

        String[] zkHostsSource = props.getProperty("source.zkHost").split(",");
        String[] zkHostsTarget = props.getProperty("target.zkHost").split(",");
        int nbThreads = Integer.valueOf(props.getProperty("nbThreads", "5"));
        String accountFiles = props.getProperty("queryLst.path");
        String sourceCollection = props.getProperty("source.collection");
        String targetCollection = props.getProperty("target.collection");

        IndexClient sourceSolr = new IndexClient(zkHostsSource);
        IndexClient targetSolr = new IndexClient(zkHostsTarget);

        InputStream stream = new FileInputStream(accountFiles);
        List<String> accountList = IOUtils.readLines(stream, "UTF-8");

        ForkJoinPool threadPool = new ForkJoinPool(nbThreads);
        accountList.stream().forEach(query -> {
            threadPool.submit(new PushTask(sourceSolr, targetSolr, sourceCollection, targetCollection, query.trim()));

        });

        int running = threadPool.getRunningThreadCount();
        int total = accountList.size();
        logger.info("{} {}", running, total);
        while (running > 0 && threadPool.isTerminated() == false) {
            Thread.sleep(100);
        }

    }

}
