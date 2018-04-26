package com.cloudera.poc.solr

import java.io.ByteArrayInputStream

import com.cloudera.poc.SolRSupport
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.{SparkConf, SparkContext}

object BatchIndexMeta {
  def main(args: Array[String]) {
 
    // function: convertToSolrDoc
    // purpose: converts an avro record to a solr input document
    def convertToSolrDoc(): SolrInputDocument = {
 
      val doc = new SolrInputDocument()
 
      // we're going to use ctr as a suffix value for any child documents
      // names and addresses, we need to increment
      //var ctr = 0
 
      doc.addField("raw_value", "solrj-test")
      doc.addField("meta_type", "solrj-test")
      doc.addField("column_name", "solrj-test")
      doc.addField("table_name", "solrj-test")
      doc.addField("file_name", "solrj-test")
      doc.addField("inserted_date", "04-23-2018")
      doc.addField("id", "2000" )
      doc
    }
 
    // function: convertToAvro
    // purpose: converts byte array to a generic avro record
    def convertToAvro(bytes : Array[Byte]) : GenericRecord = {
      val reader = new GenericDatumReader[GenericRecord]
      val is = new ByteArrayInputStream(bytes)
      val decoder = DecoderFactory.get().directBinaryDecoder(is, null);
      val datum = reader.read(null, decoder)
      datum
    }
 
    def printHelp() {
      println("Required parameters: hbase_table zookeeper_host collection_name batch_size")
      println("   e.g.  namespace:table zkhost:2181/solr collection1 100")
    }
 
    // Main starts here
 
    if (args.length != 4) {
      printHelp()
      return
    }
 
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
 
    val p_table = args(0)
    val p_zkhost = args(1)
    val p_collection = args(2)
    val p_batchsize = args(3).toInt
 
    val conf = HBaseConfiguration.create
    conf.set(TableInputFormat.INPUT_TABLE, p_table)
 
    //val hRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
     // classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      //classOf[org.apache.hadoop.hbase.client.Result]).map(tuple => tuple._2)
       //   .map(result => (Bytes.toString(result.getRow()), result.value ) )
 
    //val avroRecs = hRDD.map(result => (result._1, convertToAvro(result._2)))
    val solrDocs=sc.parallelize(List(1,2)).map(x => convertToSolrDoc() )
    //val solrDocs = avroRecs.map(x => convertToSolrDoc() )
    //println(solrDocs.collect())
    println("Number of Solr Docs:" + solrDocs.count )
 
    // Submit the RDD for indexing - provided sample code should handle this
    SolRSupport.indexDoc(p_zkhost, p_collection, p_batchsize, solrDocs)
  }
}
 
