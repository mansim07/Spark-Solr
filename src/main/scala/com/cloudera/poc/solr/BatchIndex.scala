package com.cloudera.poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.solr.common.SolrInputDocument
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
 
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory


import java.io.ByteArrayInputStream
 
object BatchIndex {
  def main(args: Array[String]) {
 
    // function: convertToSolrDoc
    // purpose: converts an avro record to a solr input document
    def convertToSolrDoc(id : String, rec: GenericRecord): SolrInputDocument = {
 
      val doc = new SolrInputDocument()
 
      // we're going to use ctr as a suffix value for any child documents
      // names and addresses, we need to increment
      var ctr = 0
 
      doc.addField("type_s", "parent")
      doc.addField("id", id )
 
      val names = rec.get("Names").asInstanceOf[org.apache.avro.generic.GenericData.Array[GenericRecord]]
      val names_iter = names.iterator
      while (names_iter.hasNext) {
          val a = names_iter.next()
 
          // first name and last name should exist
          val fname = a.get("fname")
          val lname = a.get("lname")
 
          // create child document and add to the parent
          ctr+=1
          val namesDoc = new SolrInputDocument()
          val childId = id + "." + ctr;
          namesDoc.addField("id", childId)
          namesDoc.addField("fname_s", fname )
          namesDoc.addField("lname_s", lname )
          doc.addChildDocument(namesDoc)
      }

      val addresses = rec.get("Addresses").asInstanceOf[org.apache.avro.generic.GenericData.Array[GenericRecord]]
      val addresses_iter = names.iterator
      while (addresses_iter.hasNext) {
        val a = addresses_iter.next()

        // first name and last name should exist
        val streetno = a.get("streetno")
        val streetname = a.get("streetname")
        val city = a.get("city")

        // create child document and add to the parent
        ctr+=1
        val addressesDoc = new SolrInputDocument()
        val childId = id + "." + ctr;
        addressesDoc.addField("id", childId)
        addressesDoc.addField("streetno", streetno )
        addressesDoc.addField("streetname", streetname )
        addressesDoc.addField("city", city )
        doc.addChildDocument(addressesDoc)
      }

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
 
    val hRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).map(tuple => tuple._2)
          .map(result => (Bytes.toString(result.getRow()), result.value ) )
 
    val avroRecs = hRDD.map(result => (result._1, convertToAvro(result._2)))
 
    val solrDocs = avroRecs.map(x => convertToSolrDoc(x._1,x._2) )
    println("Number of Solr Docs:" + solrDocs.count )
 
    // Submit the RDD for indexing - provided sample code should handle this
    SolRSupport.indexDoc(p_zkhost, p_collection, p_batchsize, solrDocs)
  }
}
 
