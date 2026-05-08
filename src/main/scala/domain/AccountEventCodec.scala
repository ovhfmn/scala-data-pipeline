package domain

import domain.AccountEvent._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, DecodingFailure, Encoder, Json}

object AccountEventCodec {

  // Subtype codecs

  implicit val createdEncoder: Encoder[AccountEvent.AccountCreated] = deriveEncoder

  implicit val createdDecoder: Decoder[AccountEvent.AccountCreated] = deriveDecoder

  implicit val debitedEncoder: Encoder[AccountEvent.AccountDebited] = deriveEncoder

  implicit val debitedDecoder: Decoder[AccountEvent.AccountDebited] = deriveDecoder

  implicit val creditedEncoder: Encoder[AccountEvent.AccountCredited] = deriveEncoder

  implicit val creditedDecoder: Decoder[AccountEvent.AccountDebited] = deriveDecoder

  // Polymorphic encoder

  implicit val accountEventEncoder: Encoder[AccountEvent] =
    Encoder.instance {
      case e@AccountCreated(_, _, _, _) =>
        createdEncoder(e).deepMerge(
          Json.obj(
            "eventType" -> Json.fromString("AccountCreated")
          )
        )
      case e@AccountDebited(_, _, _, _) =>
        debitedEncoder(e).deepMerge(
          Json.obj(
            "eventType" -> Json.fromString("AccountDebited")
          )
        )
      case e@AccountCredited(_, _, _, _) =>
        creditedEncoder(e).deepMerge(
          Json.obj(
            "eventType" -> Json.fromString("AccountCredited")
          )
        )
    }

  // Polymorphic decoder

  implicit val accountEventDecoder: Decoder[AccountEvent] =
    Decoder.instance { cursor =>
      cursor.get[String]("eventType").flatMap {
        case "AccountCreated" =>
          createdDecoder.tryDecode(cursor)
        case "AccountDebited" =>
          debitedDecoder.tryDecode(cursor)
        case "AccountCredited" =>
          creditedDecoder.tryDecode(cursor)

        case other => Left(
          DecodingFailure(
            s"Unknown eventType: $other",
            cursor.history
          )
        )
      }
    }
}
