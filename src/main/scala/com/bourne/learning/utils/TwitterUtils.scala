package com.bourne.learning.utils

import com.bourne.learning.twitterKeys.KeysAndSecret
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Constants, HttpHosts}
import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}

import java.util.concurrent.BlockingQueue

object TwitterUtils extends KeysAndSecret {

  def createTwitterClient(msgQueue: BlockingQueue[String], termsToFind: List[String]): Client = {
    val hosebirdHosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint = new StatusesFilterEndpoint

    import scala.collection.JavaConverters._
    val terms = termsToFind.asJava
    hosebirdEndpoint.trackTerms(terms)
    val hosebirdAuth: Authentication = new OAuth1(consumerKey, consumerSecret, token, secret)

    val builder = new ClientBuilder()
      .name("Hosebird-Client-01")
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))

    builder.build
  }
}
