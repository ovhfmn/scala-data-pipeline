package storage

import cats.effect.IO
import domain.AccountEvent
import io.circe.syntax.EncoderOps

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.{Instant, ZoneOffset, ZonedDateTime}

/** Persists batches of account events to partitioned JSONL files on the local filesystem.
 *
 * Acts as the bronze layer of the data lake — raw, immutable, append-only.
 * Downstream Spark Analytics jobs read from this layer for batch processing.
 */
object EventFileWriter {

  /** Serializes a batch of events to a JSONL file partitioned by UTC time.
   *
   * Output path structure:
   * {{{
   * /app/data/lake/year=YYYY/month=MM/day=dd/hour=HH/events-<timestamp>.jsonl
   * }}}
   *
   * Each event is written as a single flat JSON line.
   * The file is created if absent and appended to if already present —
   * safe for multiple batches landing in the same hour partition.
   *
   * Uses `IO.blocking` to avoid occupying Cats Effect compute pool threads
   * during filesystem I/O.
   *
   * @param events Batch of decoded events to persist.
   */
  def writeBatch(events: List[AccountEvent]): IO[Unit] = {
    val timestamp = Instant.now().toEpochMilli

    val now = ZonedDateTime.now(ZoneOffset.UTC)
    val yyyy = now.getYear
    val MM = f"${now.getMonthValue}%02d"
    val dd = f"${now.getDayOfMonth}%02d"
    val HH = f"${now.getHour}%02d"

    val path = Paths.get(s"/app/data/lake/year=$yyyy/month=$MM/day=$dd/hour=$HH/events-$timestamp.jsonl")
    val lines = events
      .map(_.asJson.noSpaces)
      .mkString("\n")

    IO.blocking {
      Files.createDirectories(path.getParent)

      Files.writeString(
        path,
        lines + "\n",
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND
      )
    }
  }
}