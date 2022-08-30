package com.gjosquin.example.kstreams

import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.test.TestRecord
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class SimpleLeftJoinTopologyTest extends AsyncFlatSpec with Matchers {

  "SimpleLeftJoinTopology" should "perform left-join" in {
    val simpleLeftJoinTopology = new SimpleLeftJoinTopology
    val driver = new TopologyTestDriver(simpleLeftJoinTopology.topology)
    val serde = Serdes.stringSerde

    val bills = driver.createInputTopic("bills", serde.serializer, serde.serializer)
    val payments = driver.createInputTopic("payments", serde.serializer, serde.serializer)
    val debt = driver.createOutputTopic("debt", serde.deserializer, serde.deserializer)

    val t0: Instant = Instant.now()
    bills.pipeInput(new TestRecord("fred", "100", t0))
    bills.pipeInput(new TestRecord("george", "20", t0))
    payments.pipeInput(new TestRecord("fred", "95", t0))

    // Sending an extra record with an event time sufficient to close the previous window
    payments.pipeInput(new TestRecord("percy", "0", t0.plusMillis(101)))

    val keyValues = debt.readKeyValuesToList()
    keyValues should contain theSameElementsAs Seq(
      new KeyValue[String, String]("fred", "5"),
      new KeyValue[String, String]("george", "20")
    )
  }
}
