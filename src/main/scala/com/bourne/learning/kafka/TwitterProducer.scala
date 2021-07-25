package com.bourne.learning.kafka

import com.bourne.learning.kafka.LocalKafkaProducer.createKafkaProducer
import com.bourne.learning.utils.TwitterUtils.createTwitterClient
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import scala.util.{Failure, Success, Try}

object TwitterProducer extends App {

  val logger = LoggerFactory.getLogger(TwitterProducer.getClass.getName)

  val msgQueue: BlockingQueue[String] = new LinkedBlockingQueue[String](1000)
  val termsToFind = List("kafka")
  val twitterClient = createTwitterClient(msgQueue, termsToFind)
  twitterClient.connect()

  val kafkaProducer = createKafkaProducer()

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    logger.info("stooping application")
    twitterClient.stop()
    logger.info("twitter client shut down successfully")
    kafkaProducer.close()
    logger.info("kafka client shut down successfully")
    logger.info("done ...")
  }))

  while (!twitterClient.isDone) {
    Try {
      msgQueue.poll(5L, TimeUnit.SECONDS)
    } match {
      case Success(msg) =>
        if (msg == null) logger.info("Message received is null")
        else {
          logger.info(msg)
          kafkaProducer.send(new ProducerRecord("twitter_tweets_01", null, msg), new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              if (exception != null) {
                logger.error("Something bad happened", exception)
              }
            }
          })
        }

      case Failure(exception) =>
        exception match {
          case interruptedException: InterruptedException =>
            interruptedException.printStackTrace()
            twitterClient.stop()
        }
    }
  }
  logger.info("End of application")
}
