package com.rockthejvm.bookings.actor

import akka.actor.typed.Behavior
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import com.rockthejvm.bookings.model._

// Primary persistent actor
object Hotel {

  case class State(reservations: Set[Reservation])

  // Get async command --> Modify internal state --> Persist an effect to persistence store
  def commandHandler(hotelId: String): (State, Command) => Effect[Event, State] = (state, command) =>
    command match {
      case MakeReservation(guestId, startDate, endDate, roomNumber, replyTo) =>
        val tentativeReservation = Reservation.make(guestId, hotelId, startDate, endDate, roomNumber)
        val conflictingReservation = state.reservations.find(reservation => reservation.intersect(tentativeReservation))
        if (conflictingReservation.isEmpty) {
          Effect
            .persist(ReservationAccepted(tentativeReservation)) // Persist
            .thenReply(replyTo)(s => ReservationAccepted(tentativeReservation)) // Reply to the "manager"
        } else {
          Effect.reply(replyTo)(CommandFailure("Room already booked"))
        }

      case ChangeReservation(confirmationNumber, startDate, endDate, roomNumber, replyTo) =>
        Effect.none // TODO

      case CancelReservation(confirmationNumber, replyTo) =>
        Effect.none // TODO
    }

  // After event has been persisted State of the actor will be modified
  def eventHandler(hotelId: String): (State, Event) => State = (state, event) =>
    event match {
      case ReservationAccepted(res) =>
        val newState = state.copy(reservations = state.reservations + res)
        println(s"state changed: $newState")
        newState
      case _ =>
        state // TODO
    }

  // Allows object to be invocted as function
  def apply(hotelId: String): Behavior[Command] = EventSourcedBehavior[Command, Event, State](
    persistenceId = PersistenceId.ofUniqueId(hotelId),
    emptyState = State(Set()),
    commandHandler = commandHandler(hotelId),
    eventHandler = eventHandler(hotelId)
  )
}
