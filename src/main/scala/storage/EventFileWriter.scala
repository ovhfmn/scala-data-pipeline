package storage

import cats.effect.IO
import domain.AccountEvent
import io.circe.syntax.EncoderOps

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.{Instant, ZoneOffset, ZonedDateTime}

object EventFileWriter {

  def writeBatch(
                events: List[AccountEvent]
                ): IO[Unit] = {
    val timestamp = Instant.now().toEpochMilli

    val now = ZonedDateTime.now(ZoneOffset.UTC)
    val yyyy = now.getYear
    val MM = f"${now.getMonthValue}%02d"
    val dd = f"${now.getDayOfMonth}%02d"
    val HH = f"${now.getHour}%02d"

    val path = Paths.get(s"src/data/lake/year=$yyyy/month=$MM/day=$dd/hour=$HH/events-$timestamp.jsonl")

    val lines = events
      .map(_.asJson.noSpaces)
      .mkString("\n")

    IO.blocking {
      Files.createDirectories(path.getParent)

      Files.writeString(
        path,
        lines + "\n",
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND,
      )
    }
  }
}