/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrdc.solr2solr;

import java.util.ArrayList;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

/**
 *
 * @author ludovic
 */
public interface ICallback {

    public void execute(SolrDocumentList solrDocs);

    public void execute(ArrayList<SolrInputDocument> solrDocs);

}
