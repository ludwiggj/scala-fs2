package com.ludwiggj.fs2.experiment

import cats.effect.{Blocker, ContextShift, ExitCode, IO, IOApp}
import com.ludwiggj.fs2.experiment.Processor.{Date, Log}
import fs2.{Stream, io, text}

import java.net.URL
import java.nio.file.Paths
import scala.util.matching.Regex

// Something wrong with this example

// (1) NPE if the file out.txt doesn't exist
// (2) Doesn't actually write to out.txt
trait Processor {
  def processData(sourceFile: String, sinkFile: String)(
    implicit contextShift: ContextShift[IO]
  ): Stream[IO, Unit] = {
    // resource converts the supplied resource in to a singleton stream

    // Blocker is an execution context that is safe to use for blocking operations.
    // Used in conjunction with [[ContextShift]], this type allows us to write functions
    // that require a special `ExecutionContext` for evaluation, while discouraging the
    // use of a shared, general purpose pool (e.g. the global context).

    // ContextShift provides support for shifting execution.

    // Type of Blocker[IO] is Resource[IO, Blocker]
    Stream.resource(Blocker[IO]).flatMap {  blocker =>
      val inResource: URL = getClass.getResource(sourceFile)
      val outResource: URL = getClass.getResource(sinkFile)
      io.file
        // Read all data synchronously from the file
        // at the specified `java.nio.file.Path`.
        .readAll[IO](Paths.get(inResource.toURI), blocker, 4096)  // Stream[IO, Byte]
        // Transform stream using the given pipe
        .through(text.utf8Decode)                                           // Stream[IO, String]
        .through(text.lines)                                                // Stream[IO, String]
        // filter out valid IP
        .filter(isValidIp)                                                  // Stream[IO, String]
        // map applies the specified pure function
        // to each input and emits the result
        //.map(convertToLog)                                                // Stream[IO, IO[Log]]
        .evalMap(convertToLog)                                              // Stream[IO, Log]
        .debug(log => s"Log $log")
        // Fold all inputs using an initial value `z` and supplied
        // binary operator, and emits a single element stream.
        .fold(Map.empty[String, Int]) { (map, currLog) =>                   // Stream[IO, Map[String, Int]]
          val updatedStatus = map.getOrElse(currLog.status, 0) + 1
          map + (currLog.status -> updatedStatus)
        }
        .debug(map => s"Totals $map")
        .flatMap { m => {                                                   // Stream[IO, String]
          val iterator: Iterator[String] = m.keys.map { key =>
            s"Status: ${key} has a total of ${m(key)}"
          }.iterator
          val s: Stream[IO, String] = Stream.fromIterator[IO](iterator)
          s
        }}
        .debug(total => s"Total: $total")
        .through(text.utf8Encode)                                           // Stream[IO, Byte]
        .through(
          // Writes all data to the file at the
          // specified java.nio.file.Path.
          io.file.writeAll(Paths.get(outResource.toURI), blocker)           // Stream[IO, Unit]
        )
    }
  }

  def isValidIp(line: String): Boolean = {
    //println(s"Validating: $line")
    val ipRegex: Regex = """.*?(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}).*""".r
    ipRegex.pattern.matcher(line.split(",")(0)).matches()
  }

  // Example log line:
  // 10.128.2.1,[29/Nov/2017:06:58:55,GET /login.php HTTP/1.1,200
  def convertToLog(line: String): IO[Log] = line.split(",").toList match {
    case ip :: time :: url :: status :: _ =>
      IO(Log(ip, convertToDate(time), url, status))
  }

  // Example time:
  // [29/Nov/2017:06:58:55
  def convertToDate(time: String): Date = time.substring(1).split("/").toList match {
    case date :: month :: yearAndTime :: _ =>
      yearAndTime.split(":").toList match {
        case year :: rest => Date(year.toInt, month, date.toInt, rest.mkString(":"))
      }
  }
}

object Processor extends IOApp with Processor {
  case class Log(ip: String, time: Date, url: String, status: String)
  case class Date(year: Int, month: String, date: Int, time: String)

  def run(args: List[String]): IO[ExitCode] = {
    // drain compiles the stream into a value of the target effect type `F` and
    // discards any output values of the stream.
    processData("/weblog.csv", "/out.txt").compile.drain.map(_ => ExitCode.Success)
  }
}