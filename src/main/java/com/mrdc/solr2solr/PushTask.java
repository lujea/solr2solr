/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrdc.solr2solr;

import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ludovic
 */
public class PushTask implements Callable<Boolean> {

    private IndexClient sourceSolr;
    private IndexClient targetSolr;
    private String account;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public PushTask(IndexClient source, IndexClient target, String account) {
        this.sourceSolr = source;
        this.targetSolr = target;
        this.account = account;
    }

    @Override
    public Boolean call() throws Exception {

        ICallback pushCallback = new ICallback() {
            @Override
            public void execute(SolrDocumentList solrDocs) {
                try {
                    targetSolr.indexDocuments("ma", solrDocs);
                } catch (SolrServerException ex) {
                    logger.error("SolrServerException: Failed to push documents for account: {}", account, ex);
                } catch (IOException ex) {
                    logger.error("IOException: Failed to push documents for account: {}", account, ex);
                }
            }
        ;
        };
        logger.info("Start processing account: {}", account);
        sourceSolr.queryIndex("ma", account, pushCallback);
        logger.info("Finish processing account: {}", account);

        return true;
    }

}
