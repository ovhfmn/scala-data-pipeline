package domain

import java.time.Instant
import java.util.UUID

sealed trait AccountEvent {
  def eventId: UUID
  def occuredAt: Instant
  def accountId: String
}

object AccountEvent {
  final case class AccountCreated(
                                   eventId: UUID,
                                   occuredAt: Instant,
                                   accountId: String,
                                   initialBalance: BigDecimal
                                 ) extends AccountEvent

  final case class AccountDebited(
                                   eventId: UUID,
                                   occuredAt: Instant,
                                   accountId: String,
                                   amount: BigDecimal
                                 ) extends AccountEvent

  final case class AccountCredited(
                                    eventId: UUID,
                                    occuredAt: Instant,
                                    accountId: String,
                                    amount: BigDecimal
                                  ) extends AccountEvent
}



