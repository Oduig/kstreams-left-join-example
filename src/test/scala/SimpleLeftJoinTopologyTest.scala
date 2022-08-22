package com.gjosquin.example.kstreams

import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util

class SimpleLeftJoinTopologyTest extends AsyncFlatSpec with Matchers {

  "SimpleLeftJoinTopology" should "perform left-join" in {
    val simpleLeftJoinTopology = new SimpleLeftJoinTopology
    val driver = new TopologyTestDriver(simpleLeftJoinTopology.topology)
    val serde = Serdes.stringSerde

    val bills = driver.createInputTopic("bills", serde.serializer, serde.serializer)
    val payments = driver.createInputTopic("payments", serde.serializer, serde.serializer)
    val debt = driver.createOutputTopic("debt", serde.deserializer, serde.deserializer)

    bills.pipeInput("fred", "100")
    bills.pipeInput("george", "20")
    payments.pipeInput("fred", "95")

    // When in doubt, sleep twice
    driver.advanceWallClockTime(Duration.ofMillis(500))
    Thread.sleep(500)

    val keyValues = debt.readKeyValuesToList()
    keyValues should contain theSameElementsAs Seq(
      new KeyValue[String, String]("fred", "5"),
      new KeyValue[String, String]("george", "20") // Missing a produced record even after timeout
    )
  }
}
