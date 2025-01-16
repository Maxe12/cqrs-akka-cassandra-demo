package com.rockthejvm.bookings.playground

import akka.actor.typed.ActorSystem

import java.sql.Date
import scala.concurrent.duration._

import akka.actor.typed.scaladsl.Behaviors
import com.rockthejvm.bookings.actor.Hotel
import com.rockthejvm.bookings.model.MakeReservation

object HotelDemo {
  def main(args: Array[String]): Unit = {
    val simpleLogger = Behaviors.receive[Any] { (ctx, message) =>
      ctx.log.info(s"[logger] $message")
      Behaviors.same
    }

    val root = Behaviors.setup[String] { ctx =>
      val logger = ctx.spawn(simpleLogger, "logger")
      val hotel = ctx.spawn(Hotel("testHotel"), "testHotel")

      hotel ! MakeReservation("max", Date.valueOf("2024-01-16"), Date.valueOf("2024-01-25"), 101, logger) // Reply to is simple logger
      Behaviors.empty
    }

    val system = ActorSystem(root, "DemoHotel")
    import system.executionContext
    system.scheduler.scheduleOnce(5.seconds, () => system.terminate())
  }
}
