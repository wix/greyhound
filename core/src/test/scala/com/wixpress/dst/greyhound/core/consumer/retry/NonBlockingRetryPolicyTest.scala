package com.wixpress.dst.greyhound.core.consumer.retry

import org.specs2.mutable.SpecificationWithJUnit

class NonBlockingRetryPolicyTest extends SpecificationWithJUnit {

  "NonBlockingRetryPolicyTest" should {
    "retryTopicsFor should not contain original topic" in {
      val topic = "some-topic"
      NonBlockingRetryPolicy("group",None).retryTopicsFor(topic) must not(contain(topic))
    }

  }
}
