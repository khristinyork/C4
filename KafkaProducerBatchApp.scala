package com.carrefour.ingestion.agora


import java.util._
import java.util.{Collections, Properties, function, _}

import com.carrefour.ingestion.agora.KafkaProducerApp.LOG
import com.carrefour.ingestion.agora.controller.{KafkaJobSettings, KafkaJobSettingsLoader}
import com.carrefour.ingestion.agora.util.{Constants, DateUtil, PropertiesUtil}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, KafkaAdminClient, ListConsumerGroupOffsetsOptions, ListConsumerGroupOffsetsResult}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.acl.{AccessControlEntryFilter, AclBinding, AclBindingFilter}
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.{KafkaFuture, TopicPartition}
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.utils.{SystemTime, Time}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

import kafka.common.TopicAlreadyMarkedForDeletionException

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.parsing.json._
import scala.collection.mutable.HashMap

/**
  * Productor de kafka que envía mensajes con los identificadores disponibles que se pueden usar desde el front.
  */
object KafkaProducerBatchApp {
  val LOG: org.slf4j.Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    LOG.info("*************** Inicio KafkaProducerApp")

    KafkaJobSettingsLoader.parse(args, KafkaJobSettings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings => {
        /*val spark = SparkSession.builder()
          .enableHiveSupport()
          .config("spark.dynamicAllocation.enabled", "false")
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .config("spark.sql.hive.convertMetastoreParquet", "false")
          .config("hive.exec.max.dynamic.partitions", "100000")
          .config("hive.exec.max.dynamic.partitions.pernode", "20000")
          .config("spark.sql.parquet.compression.codec", "snappy")
          .config("parquet.compression", "snappy")
          .config("spark.logConf", value = true)
          .getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(2))*/


        val adminClient=AdminClient.create(initPropiedadesAdminClient)
        var mapPaticionOffset =scala.collection.mutable.Map[TopicPartition,Long]()
        var mapPaticionACNOffset =scala.collection.mutable.Map[TopicPartition,Long]()
        val result = adminClient.listConsumerGroupOffsets("id_sequence_coupon_agora_batch-gid", new ListConsumerGroupOffsetsOptions());
        //val resultACN = adminClient.listConsumerGroupOffsets("id_sequence_coupon_agora_batch-gid", new ListConsumerGroupOffsetsOptions());
        val resultACN = adminClient.listConsumerGroupOffsets("id_sequence_coupon_agora_batch-gid", new ListConsumerGroupOffsetsOptions());
        //"id_sequence_coupon_agora_batch-gid"

        geCurrentOffsetFromPartitions(result,mapPaticionOffset)
        geCurrentOffsetACNPartitions(resultACN,mapPaticionACNOffset)
        val listPaticionOffset    = mapPaticionOffset.toList
        val sizePaticionOffset    = mapPaticionOffset.size
        val listPaticionACNOffset = mapPaticionACNOffset.toList
        val sizePaticionACNOffset = mapPaticionACNOffset.size



        var suma:Long=0
        for (i <- 0 to sizePaticionOffset.-(1)) {
         if (listPaticionOffset(i)._1.equals(listPaticionACNOffset(i)._1)) {
            val resta=(listPaticionOffset(i)._2).-(listPaticionACNOffset(i)._2)
            System.out.println("Contenido lista :" + resta)
           suma+=resta
          }
        }
        System.out.print("suma:"+suma)






        //recuperamos los offset del grupo en elñ que creamos



           val initialValue: Long = getLastValue

           val lastValue: Long = initialValue + settings.numIdsToGenerate
           val producer = new KafkaProducer[String, String](iniciarPropiedadesProductor)
           LOG.warn(s"*************** El valor inicial es: $initialValue y el final es $lastValue")
           if (Long.MaxValue - initialValue < settings.numIdsToGenerate) {
             LOG.warn(s"*************** El valor del topic: ${PropertiesUtil.getProperty("kafka.topic")}")
             for (id <- initialValue to Long.MaxValue) {
               val msg = s"""{"id": $id, "timestamp": "${DateUtil.getActualDateyyyyMMddHHmmss()}"}"""
               val data = new ProducerRecord[String, String](PropertiesUtil.getProperty("kafka.topic"), "producer", msg)
               val aux = producer.send(data)
               LOG.warn(s"--- Introducimos el mensaje $msg con offset ${aux.get.offset}")
             }
             LOG.warn(s"*************** El valor inicial es: $initialValue y el final es $lastValue")
             for (id <- 1.toLong to settings.numIdsToGenerate) {
               LOG.warn("id <- 1.toLong to settings.numIdsToGenerate")
               val msg = s"""{"id": $id, "timestamp": "${DateUtil.getActualDateyyyyMMddHHmmss()}"}"""
               val data = new ProducerRecord[String, String](PropertiesUtil.getProperty("kafka.topic"), "producer", msg)
               val aux = producer.send(data)
               LOG.warn(s"--- Introducimos el mensaje $msg con offset ${aux.get.offset}")
             }
           } else {
             for (id <- initialValue to lastValue) {
               LOG.warn("id <- 1.toLong to settings.numIdsToGenerate")
               val msg = s"""{"id": $id, "timestamp": "${DateUtil.getActualDateyyyyMMddHHmmss()}"}"""
               val data = new ProducerRecord[String, String](PropertiesUtil.getProperty("kafka.topic"), "producer", msg)
               val aux = producer.send(data)
               LOG.warn(s"--- Introducimos el mensaje $msg con offset ${aux.get.offset}")
             }
           }
           producer.close()
      }
    }
  }


  def geCurrentOffsetFromPartitions( resultado:ListConsumerGroupOffsetsResult,mapPaticionOffset:scala.collection.mutable.Map[TopicPartition,Long]): Map[TopicPartition,Long]={
    val res=resultado.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
    res.entrySet().forEach(new java.util.function.Consumer[java.util.Map.Entry[TopicPartition, OffsetAndMetadata]]() {
      def  accept(topicPartitionOffsetAndMetadataEntry:java.util.Map.Entry[TopicPartition, OffsetAndMetadata] ) {
        val topicParticion:TopicPartition=topicPartitionOffsetAndMetadataEntry.getKey()
        val offsetCurrent:Long=topicPartitionOffsetAndMetadataEntry.getValue().offset()
        mapPaticionOffset+=(topicParticion->offsetCurrent)
        System.out.println(topicParticion)
        System.out.println(offsetCurrent)
      }
    })
    return mapPaticionOffset;
  }

  def geCurrentOffsetACNPartitions( resultado:ListConsumerGroupOffsetsResult,mapPaticionACNOffset:scala.collection.mutable.Map[TopicPartition,Long]): Map[TopicPartition,Long]={
    val res=resultado.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
    res.entrySet().forEach(new java.util.function.Consumer[java.util.Map.Entry[TopicPartition, OffsetAndMetadata]]() {
      def  accept(topicPartitionOffsetAndMetadataEntry:java.util.Map.Entry[TopicPartition, OffsetAndMetadata] ) {
        val topicParticion:TopicPartition=topicPartitionOffsetAndMetadataEntry.getKey()
        val offsetCurrent:Long=topicPartitionOffsetAndMetadataEntry.getValue().offset()
        val offsetCurrentACN=offsetCurrent.-(12.toLong)
        mapPaticionACNOffset+=(topicParticion-> offsetCurrentACN)
        System.out.println(topicParticion)
        System.out.println(offsetCurrentACN)
      }
    })
    return mapPaticionACNOffset;
  }

  /**
    * Recupera el último valor de ID de los generados.
    *
    * @return Valor.
    */
  def getLastValue: Long = {

    var initialValue: Long = 1
    val consumer = new KafkaConsumer[String, String](iniciarPropiedadesConsumidor)
    LOG.warn("*****************************************************************consumer.subscribe")
    LOG.warn(s"${PropertiesUtil.getProperty("kafka.topic")}")
    consumer.subscribe(Collections.singletonList(PropertiesUtil.getProperty("kafka.topic")))

    val records = consumer.poll(Constants.KAFKA_POLL_TIMEOUT)
    if (!records.isEmpty) {
      var elem: ConsumerRecord[String, String] = null
      for (record <- records) {
        elem = record
        LOG.warn(s"""************ offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}""")
      }

      val jsonObject: Option[Map[String, Any]] = JSON.parseFull(elem.value).asInstanceOf[Option[Map[String, Any]]]
      LOG.warn("*************************************")
      LOG.warn("" + jsonObject)
      initialValue = getValueFromJson(jsonObject, "id") + 1
    }
    consumer.commitSync()
    consumer.close()

    initialValue
  }

  /**
    * Recupera el valor del mensaje.
    *
    * @param parsedJson Mensaje en formato json.
    * @param key        Clave cuyo valor se recuperará.
    * @return Valor.
    */
  def getValueFromJson(parsedJson: Option[Map[String, Any]], key: String): Long = {
    LOG.info(parsedJson.toString)
    parsedJson match {
      case Some(m) => m(key) match {
        case d: Int => d.toLong
        case d: Long => d
        case d: Double => d.toLong
      }
      case None => 0
    }
  }

  /**
    * Iniciamos las configuraciones necesarias para el productor de kafka.
    *
    * @return Propiedades.
    */
  def iniciarPropiedadesConsumidor: Properties = {
    System.out.print("bootstrpserver: "+PropertiesUtil.getProperty("kafka.bootstrap.servers.config"));
    val consumerProperties = new Properties
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, PropertiesUtil.getProperty("kafka.auto.offset.reset.config"))
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getProperty("kafka.bootstrap.servers.config"))
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, PropertiesUtil.getProperty("kafka.group.id.config"))
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, PropertiesUtil.getProperty("kafka.enable.auto.commit.config"))
    consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, PropertiesUtil.getProperty("kafka.session.timeout.ms.config"))
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, PropertiesUtil.getProperty("kafka.key.deserializer.class.config"))
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PropertiesUtil.getProperty("kafka.value.deserializer.class.config"))
    consumerProperties.put("security.protocol", PropertiesUtil.getProperty("kafka.security.protocol"))
    consumerProperties.put("sasl.kerberos.service.name", PropertiesUtil.getProperty("kafka.kerberos.service.name"))
    consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, PropertiesUtil.getProperty("kafka.client.id_config"))
    LOG.info("*************************************iniciarPropiedadesConsumidor*****************************************************************")
    consumerProperties
  }

  def initPropiedadesAdminClient: Properties = {
    val adminClientProperties = new Properties
    adminClientProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getProperty("kafka.bootstrap.servers.config"))
    adminClientProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, PropertiesUtil.getProperty("kafka.client.id_config"))
    adminClientProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, PropertiesUtil.getProperty("kafka.key.deserializer.class.config"))
    adminClientProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PropertiesUtil.getProperty("kafka.value.deserializer.class.config"))
  //  adminClientProperties.put(ConsumerConfig.GROUP_ID_CONFIG, PropertiesUtil.getProperty("kafka.group.id.config"))
    adminClientProperties.put("security.protocol", PropertiesUtil.getProperty("kafka.security.protocol"))
    adminClientProperties.put("sasl.kerberos.service.name", PropertiesUtil.getProperty("kafka.kerberos.service.name"))
    adminClientProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, PropertiesUtil.getProperty("kafka.client.id_config"))
    LOG.info("*************************************adminClientProperties*****************************************************************")
    adminClientProperties
  }
  /**
    * Iniciamos las configuraciones necesarias para el productor de kafka.
    *
    * @return Propiedades.
    */
  def iniciarPropiedadesProductor: Properties = {
    val producerProperties = new Properties()
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getProperty("kafka.bootstrap.servers.config"))
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getProperty("kafka.key.serializer.class.config"))
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getProperty("kafka.value.serializer.class.config"))
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "all")
    //producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, "1")
    producerProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "CuponIdProducer")
    producerProperties.put("security.protocol", PropertiesUtil.getProperty("kafka.security.protocol"))
    producerProperties.put("sasl.kerberos.service.name", PropertiesUtil.getProperty("kafka.kerberos.service.name"))

    producerProperties
  }

}

