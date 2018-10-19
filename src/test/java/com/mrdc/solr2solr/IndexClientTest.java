/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrdc.solr2solr;

import com.mrdc.solr2solr.IndexClient;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ludovic
 */
public class IndexClientTest {

    private final Logger logger = LoggerFactory.getLogger(IndexClientTest.class);

    public IndexClientTest() {
    }

    /**
     * Test of queryIndex method, of class IndexClient.
     */
    @Test
    public void testQueryIndex() throws Exception {
        System.out.println("queryIndex");
        String account = "hadoop@apache.org";
        String[] hosts = new String[]{"10.10.40.183:32000/solr"};
        String[] targetHost = new String[]{"10.10.23.218:32000/solr"};

        IndexClient instance = new IndexClient(hosts);
        IndexClient target = new IndexClient(targetHost);

//        ICallback pushCallback = new ICallback() {
//            @Override
//            public void execute(SolrDocumentList solrDocs) {
//                try {
//                    target.indexDocuments("ma", solrDocs);
//                } catch (SolrServerException ex) {
//                    logger.error("SolrServerException: failed to push documents", ex);
//                } catch (IOException ex) {
//                    logger.error("IOException: failed to push documents", ex);
//                }
//            }
//        ;
//        };
//        
//        instance.queryIndex("ma", account, pushCallback);

    }

}
