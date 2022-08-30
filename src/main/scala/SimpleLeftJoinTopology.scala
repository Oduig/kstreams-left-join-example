package com.gjosquin.example.kstreams

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{Consumed, JoinWindows}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, Produced, StreamJoined}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.time.Duration


class SimpleLeftJoinTopology {

  private implicit val consumed: Consumed[String, String] =
    Consumed.`with`(Serdes.stringSerde, Serdes.stringSerde)
  private implicit val streamJoined: StreamJoined[String, String, String] =
    StreamJoined.`with`(Serdes.stringSerde, Serdes.stringSerde, Serdes.stringSerde)
  private implicit val produced: Produced[String, String] =
    Produced.`with`(Serdes.stringSerde, Serdes.stringSerde)

  val topology: Topology = {
    val builder = new StreamsBuilder

    val bills: KStream[String, String] = builder.stream("bills")
    val payments: KStream[String, String] = builder.stream("payments")

    bills
      .leftJoin(payments)(
        {
          case (billValue, null) => billValue
          case (billValue, paymentValue) => (billValue.toInt - paymentValue.toInt).toString
        },
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(100))
      )
      .to("debt")

    builder.build()
  }
}
