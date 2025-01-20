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
          Effect.reply(replyTo)(CommandFailure("Room already booked."))
        }

      case ChangeReservation(confirmationNumber, startDate, endDate, roomNumber, replyTo) =>
        // find old reservation
        // if no reservation found => failure
        // create new tentative reservation
        // fin if the conflict, if so => failure
        // else persist Reservation find.
        val oldReservationOption = state.reservations.find(_.confirmationNumber == confirmationNumber)
        val newReservationOption = oldReservationOption.map(res => res.copy(startDate = startDate, endDate = endDate, roomNumber = roomNumber))
        val updateReservationEventOption = oldReservationOption.zip(newReservationOption).map(ReservationUpdated.tupled)
        val conflictingReservationOption = newReservationOption.flatMap(
          tentativeReservation => state.reservations.find(r => r.confirmationNumber != confirmationNumber && r.intersect(tentativeReservation))
        )
        (updateReservationEventOption, conflictingReservationOption) match {
          case (None, _) =>
            Effect.reply(replyTo)(CommandFailure(s"Cannot update reservation ${confirmationNumber}: not found"))
          case (_, Some(_)) =>
            Effect.reply(replyTo)(CommandFailure(s"Cannot update reservation ${confirmationNumber}: conflicting reservation"))
          case (Some(updatedReservation), None) =>
            Effect.persist(updatedReservation)
            .thenReply(replyTo)(s => updatedReservation)
        }

      case CancelReservation(confirmationNumber, replyTo) =>
        val reservationToBeCancelled = state.reservations.find(_.confirmationNumber == confirmationNumber)
        reservationToBeCancelled match {
          case Some(reservation) =>
            Effect.persist(ReservationCanceled(reservation))
              .thenReply(replyTo)(s => ReservationCanceled(reservation))
          case None =>
            Effect.reply(replyTo)(CommandFailure(s"Cannot cancel reservation ${confirmationNumber}: not found"))
        }
    }

  // After event has been persisted State of the actor will be modified
  def eventHandler(hotelId: String): (State, Event) => State = (state, event) =>
    event match {
      case ReservationAccepted(res) =>
        val newState = state.copy(reservations = state.reservations + res)
        println(s"state changed: $newState")
        newState
      case ReservationUpdated(oldReservation, newReservation) =>
        val newState = state.copy(reservations = state.reservations - oldReservation + newReservation)
        println(s"state changed: $newState")
        newState
      case ReservationCanceled(deletedReservation) =>
        val newState = state.copy(reservations = state.reservations - deletedReservation)
        println(s"state changed: $newState")
        newState
    }

  // Allows object to be invocted as function
  def apply(hotelId: String): Behavior[Command] = EventSourcedBehavior[Command, Event, State](
    persistenceId = PersistenceId.ofUniqueId(hotelId),
    emptyState = State(Set()),
    commandHandler = commandHandler(hotelId),
    eventHandler = eventHandler(hotelId)
  )
}
