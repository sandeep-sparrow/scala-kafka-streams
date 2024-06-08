package com.rockthejvm

import io.circe.generic.auto._
import io.circe.{Decoder, Encoder}
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties

object KafkaStreams {

  // Updating of orders, discounts and payment in kafka topics...
  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status = String

    case class Order(orderId: OrderId, userId: UserId, products: List[Product], amount: Double) // never use double for money
    case class Discount(profile: Profile, amount: Double)
    case class Payment(orderId: OrderId, status: Status)
  }

  object Topics {
    val OrdersByUser = "orders-by-user"
    val DiscountProfilesByUser = "discount-profiles-by-user"
    val Discounts = "discounts"
    val Orders = "orders"
    val Payments = "payments"
    val PaidOrders = "paid-orders"
  }

  def generateKafkaTopicCommands(): Unit = {
    List(
      "orders-by-user",
      "discount-profiles-by-user",
      "discounts",
      "orders",
      "payments",
      "paid-orders"
    ).foreach {
      topic => println(s"kafka-topics --bootstrap-server localhost:9092 --topic ${topic} --create")
    }
  }

  // source = emits elements of a particular types
  // flow = transforms elements along the way (e.g: map)
  // sink = "injects" elements

  import Domain._
  import Topics._

  // create serializer & deserializer for all our Data Structures
  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes()
    val deserializer = (bytes: Array[Byte]) => {
      val string = new String(bytes)
      decode[A](string).toOption
    }
    Serdes.fromFn[A](serializer, deserializer)
  }

  def main(args: Array[String]): Unit = {

    // topology - used for creating kafka stream - which define/describe how the data flows in kafka streams
    val builder = new StreamsBuilder() // kafka stream stateful DS, that will allow us to add, kafka stream component to this topology

    // FUNDAMENTAL ELEMENTS We can add to TOPOLOGY are KStream, KTable, GlobalKTable

    // KStream - COMPONENT 1 (basic component - simple linear stream of data)
    val usersOrdersStream: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUser)

    // KTable - COMPONENT 2 (elements are maintained inside broker) - Table of Profile
    // distributed table to single node / partition
    val userProfilesTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfilesByUser)

    // GlobalKTable - COMPONENT 3 ( copied to all the nodes ) BEST PRACTICE : few data flow...
    val discountProfilesGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](Discounts)

    // KStream transformation: filter, map, mapValues, flatMap, flatMapValues
    val expensiveOrders = usersOrdersStream.filter {(userId, order) =>
      order.amount > 1000
    }

    val listOfProducts = usersOrdersStream.mapValues { order =>
      order.products
    }

    val productsStreams = usersOrdersStream.flatMapValues(_.products)

    // join
    val orderWithUserProfile = usersOrdersStream.join(userProfilesTable) { (order, profile) =>
      (order, profile)
    }

    val discountedOrdersStream = orderWithUserProfile.join(discountProfilesGTable) (
      { case (userId, (order, profile)) => profile}, // key of the join - picked from the "left" stream
      {case ((order, profile), discount) => order.copy(amount = order.amount - discount.amount)} // values of the matched records
    )

    // pick another identifier
    val ordersStream = discountedOrdersStream.selectKey((userId, order) => order.orderId)
    val paymentsStream  = builder.stream[OrderId, Payment](Payments)

    val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))
    val joinOrdersPayments = (order: Order, payment: Payment) => if(payment.status == "PAID") Option(order) else Option.empty[Order]

    val ordersPaid = ordersStream.join(paymentsStream) (joinOrdersPayments, joinWindow)
      .flatMapValues(maybeOrder => maybeOrder.toIterable)

    // sink
    ordersPaid.to(PaidOrders)

    val topology = builder.build()

    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    println(topology.describe())
  }
}
