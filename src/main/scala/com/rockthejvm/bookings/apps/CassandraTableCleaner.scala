package com.rockthejvm.bookings.apps

import akka.stream.scaladsl.Source
import java.time.LocalDate

object CassandraTableCleaner {
  def clearTables(): Unit = {
    val daysFree = Source (
      for {
        hotelId <- List("testHotel")
        roomNumber <- 1 to 100
        day <- (0 until 365).map(LocalDate.of(2023, 1, 1).plusDays(_))
      } yield (hotelId, roomNumber, day)
    )

    val clearRooms = daysFree.mapAsync(8) {
      case (hotelId, roomNumber, day) =>

    }
  }
}
