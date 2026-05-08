package domain

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

object AccountEventCodec {

  implicit val createdEncoder: Encoder[AccountEvent.AccountCreated] = deriveEncoder

  implicit val createdDecoder: Encoder[AccountEvent.AccountCreated] = deriveEncoder

  implicit val debitedEncoder: Encoder[AccountEvent.AccountDebited] = deriveEncoder

  implicit val debitedDecoder: Encoder[AccountEvent.AccountDebited] = deriveEncoder

  implicit val creditedEncoder: Encoder[AccountEvent.AccountDebited] = deriveEncoder

  implicit val creaditedDecoder: Encoder[AccountEvent.AccountDebited] = deriveEncoder

}
