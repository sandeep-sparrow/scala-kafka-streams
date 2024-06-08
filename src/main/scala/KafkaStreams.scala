package com.rockthejvm

import java.io.ObjectInputFilter.Status

object KafkaStreams {

  // updating of orders, discounts and payment in kafka topics...
  object Domain {
    private type UserId = String
    private type Profile = String
    private type Product = String
    private type OrderId = String
    private type Status = String

    case class Order(orderId: OrderId, userId: UserId, products: List[Product], amount: Double) // never use double for money
    case class Discount(profile: Profile, amount: Double)
    case class Payment(orderId: OrderId, status: Status)
  }

  object Topics {
    val OrderByUser = "orders-by-user"
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

  def main(args: Array[String]): Unit = {

  }
}