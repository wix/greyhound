package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import org.specs2.mutable.SpecificationWithJUnit

class RecordConsumerConfigTest extends SpecificationWithJUnit {

  "RecordConsumerConfig" should {
    "kafkaAuthProperties" in {
      val config = RecordConsumerConfig.apply("localhost:6667","group", Topics(Set("topic")), extraProperties = Map(
        "security.protocol" -> "SASL_SSL",
        "sasl.mechanism" -> "PLAIN",
        "auto.offset.reset" -> "earliest",
        "ssl.truststore.location" -> "trust.location",
        "ssl.truststore.password" -> "trust.password",
        "ssl.keystore.location" -> "keyStore.location",
        "ssl.keystore.password" -> "keyStore.password",
        "ssl.key.password" -> "key.password"
      ))
      config.kafkaAuthProperties mustEqual(Map(
        "security.protocol" -> "SASL_SSL",
        "sasl.mechanism" -> "PLAIN",
        "ssl.truststore.location" -> "trust.location",
        "ssl.truststore.password" -> "trust.password",
        "ssl.keystore.location" -> "keyStore.location",
        "ssl.keystore.password" -> "keyStore.password",
        "ssl.key.password" -> "key.password"
      ))
    }
  }
}
