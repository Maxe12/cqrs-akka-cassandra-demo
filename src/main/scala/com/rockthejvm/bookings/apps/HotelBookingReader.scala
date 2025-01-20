package com.rockthejvm.bookings.apps

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.PersistenceQuery
import akka.stream.scaladsl.Sink
import com.rockthejvm.bookings.model._

// CQRS Read Site
// Akka Persistence Query
// Monitor all the events associated to a specific persistence ID
object HotelBookingReader {
  implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "HotelBookingReader")

  // Read journal
  val readJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  // Read journal can fetch: all persistence IDs => Since those actors can update over time this is a Source Type
  val persistenceIds = readJournal.persistenceIds()
  val consumtionSink = Sink.foreach(println)
  val connectedGraph = persistenceIds.to(consumtionSink)

  def makeReservation(reservation: Reservation) = {

  }
  // Read journal can also fetch all the events for a persistence ID
  val eventsForTestHotel = readJournal
    .eventsByPersistenceId("testHotel", 0, Long.MaxValue)
    .map(_.event)
    .map({
      case ReservationAccepted(res) =>
        println(s"MAKING RESERVATION: $res")
      case ReservationUpdated(oldReservation, newReservation) =>
        println(s"CHANGING RESERVATION: from $oldReservation $newReservation")
      case ReservationCanceled(res) =>
        println(s"CANCELLING RESERVATION: $res")
    })
  def main(args: Array[String]): Unit = {
    // Will print all the connected hotels (testHotel)
    //    connectedGraph.run()
    // Will print all the events for test hotel (notice sink ignore because we already map to println
    eventsForTestHotel.to(Sink.ignore).run()
  }
}
