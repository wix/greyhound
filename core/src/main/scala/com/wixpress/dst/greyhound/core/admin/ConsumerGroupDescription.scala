package com.wixpress.dst.greyhound.core.admin

import com.wixpress.dst.greyhound.core.TopicPartition
import org.apache.kafka.clients.admin
import org.apache.kafka.common.ConsumerGroupState

import scala.collection.JavaConverters._

case class ConsumerGroupDescription(groupId: String, members: Set[MemberDescription], state: ConsumerGroupState)
case class MemberDescription(consumerId: String, clientId: String, host: String, assignment: Set[TopicPartition])

object ConsumerGroupDescription {
  def apply(groupDescription: admin.ConsumerGroupDescription): ConsumerGroupDescription = {
    ConsumerGroupDescription(
      groupDescription.groupId(),
      groupDescription.members().asScala.map(MemberDescription.apply).toSet,
      groupDescription.state()
    )
  }
}
object MemberDescription {
  def apply(member: admin.MemberDescription): MemberDescription = {
    MemberDescription(
      member.consumerId(),
      member.clientId(),
      member.host(),
      member.assignment().topicPartitions().asScala.map(TopicPartition.apply).toSet
    )
  }
}
