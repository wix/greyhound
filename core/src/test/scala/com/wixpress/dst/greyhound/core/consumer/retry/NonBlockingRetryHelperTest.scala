package com.wixpress.dst.greyhound.core.consumer.retry

import org.specs2.mutable.SpecificationWithJUnit

class NonBlockingRetryHelperTest extends SpecificationWithJUnit {

  "NonBlockingRetryHelper" should {
    "retryTopicsFor should not contain original topic" in {
      val topic = "some-topic"
      NonBlockingRetryHelper("group",None).retryTopicsFor(topic) must not(contain(topic))
    }

  }
}
