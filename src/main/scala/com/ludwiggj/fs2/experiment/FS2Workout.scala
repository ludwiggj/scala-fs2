package com.ludwiggj.fs2.experiment

import cats.effect.IO
import fs2.{Pure, Stream}

import java.io.{BufferedReader, File, FileReader}

object FS2Workout {
  // Examples taken from https://levelup.gitconnected.com/how-to-write-data-processing-application-in-fs2-2b6f84e3939c

  def pureStreamExample(): Unit = {
    // A pure stream, without an effect type
    val s: Stream[Pure, Int] = Stream(1, 2, 3)

    // Runs this pure stream and returns the emitted elements in a list.
    // This method is only available on pure streams.
    println(s.toList)
  }

  def ioStreamExample(): Unit = {
    // IO suspends a synchronous side effect in `IO`.
    // Any exceptions thrown by the effect will be caught and sequenced into the `IO`.
    val io: IO[Int] = IO(1)

    // eval creates a single element stream that gets its value by evaluating the supplied effect.
    // If the effect fails, the returned stream fails.
    val ios: Stream[IO, Int] = Stream.eval(io)

    // compile gets a projection of this stream that allows converting it to an `F[..]` in a number of ways.
    val iosCompiled = ios.compile

    // toVector compiles this stream into a value of the target effect type `F` by logging the output values
    // to a `Vector`. Equivalent to `to[Vector]`. When this method has returned, the stream has not begun
    // execution -- this method simply compiles the stream down to the target effect type.
    val ioVector1: IO[Vector[Int]] = iosCompiled.toVector
    val ioVector2: IO[Vector[Int]] = iosCompiled.to(Vector)

    // unsafeRunSync produces the result by running the encapsulated effects as impure side effects.
    // If any component of the computation is asynchronous, the current thread will block awaiting
    // the results of the async computation. Any exceptions raised within the effect will be
    // re-thrown during evaluation.
    val vector1: Vector[Int] = ioVector1.unsafeRunSync()

    println(s"Vector 1 = $vector1")
    println(s"Vector 2 = ${ioVector2.unsafeRunSync()}")

    // toList compiles this stream in to a value of the target effect type `F` by logging the output values
    // to a `List`. Equivalent to `to[List]`. When this method has returned, the stream has not begun
    // execution -- this method simply compiles the stream down to the target effect type.
    println(s"List 1 = ${iosCompiled.toList.unsafeRunSync()}")
    println(s"List 2 = ${iosCompiled.to(List).unsafeRunSync()}")
  }

  def ioStreamWithRethrownExceptionExample(): Unit = {
    val io: IO[Int] = IO(throw new RuntimeException)
    val ios: Stream[IO, Int] = Stream.eval(io)
    val iosCompiled = ios.compile
    println("About to calculate result of IO containing exception. Watch out!")
    println(s"List = ${iosCompiled.toList.unsafeRunSync()}")
  }

  def covaryOutputExample(): Unit = {
    val s: Stream[Pure, Some[Int]] = Stream(Some(1), Some(2), Some(3))

    // covary lifts this stream to the specified effect type.
    val s1: Stream[Pure, Option[Int]] = s.covaryOutput[Option[Int]]
    println(s1.toVector)
  }

  def ioStreamWithRethrownExceptionExample2(): Unit = {
    // Cannot say anything about the type contained in the IO because we are just throwing an exception
    val io: IO[Nothing] = IO(throw new RuntimeException)
    val iosNothing: Stream[IO, Nothing] = Stream.eval(io)

    // covaryOutput can change the type to the desired type
    val iosInt: Stream[IO, Int] = iosNothing.covaryOutput[Int]
    val iosCompiled = iosInt.compile
    println("About to calculate result of IO containing exception. Watch out!")
    println(s"List = ${iosCompiled.toList.unsafeRunSync()}")
  }

  def ioStreamWithHandledExceptionExample(): Unit = {
    val io: IO[Int] = IO(throw new RuntimeException)
    val ios: Stream[IO, Int] = Stream.eval(io)
    val iosCompiled = ios.compile
    val ioList: IO[List[Int]] = iosCompiled.toList

    // attempt materializes any sequenced exceptions into value space, where they may be handled.
    val ioEither: IO[Either[Throwable, List[Int]]] = ioList.attempt

    println("About to calculate result of IO containing exception. Watch out!")
    println(s"List = ${ioEither.unsafeRunSync()}")
  }

  def anotherStreamExample(): Unit = {
    val s: Stream[IO, Int] = Stream.eval(IO(1))
    val s2: Stream[Pure, Int] = Stream(2, 3, 4)

    // Hmm, interesting type signatures when combining streams
    // Q - Is each element of s2 wrapped in an IO?
    val s3: Stream[IO, Int] = s.append(s2)

    println(s3.compile.toVector.unsafeRunSync)
  }

  def acquireFileExample(): Unit = {
    // bracket creates a stream that emits a resource allocated by an effect, ensuring the resource is
    // eventually released regardless of how the stream is used. A typical use case for bracket is
    // working with files or network sockets. The resource effect opens a file and returns a reference
    // to it. One can then flatMap on the returned Stream to access file, e.g to read bytes and
    // transform them into some stream of elements (e.g., bytes, strings, lines, etc.). The `release`
    // action then closes the file once the result Stream terminates, even in case of interruption or
    // errors.
    val s: Stream[IO, BufferedReader] = Stream.bracket {
      // Type signature of the acquire is IO[BufferedReader]
      // Type signature of the release is BufferedReader => IO[Unit]
      IO {
        new BufferedReader(new FileReader(new File("src/main/resources/weblog.csv")))
      }
    }(f => IO(f.close()))

    // Running via the following command:
    s.compile.toList.unsafeRunSync()
  }

  def infiniteStreamExample(): Unit = {
    val s: Stream[Pure, Int] = Stream.constant(42)
    val s1: Stream[Pure, Int] = s.take(5)
    println(s1.toList)
  }

  def main(args: Array[String]): Unit = {
    pureStreamExample()
    ioStreamExample()
    // ioStreamWithRethrownExceptionExample()
    covaryOutputExample()
    // ioStreamWithRethrownExceptionExample2()
    ioStreamWithHandledExceptionExample()
    anotherStreamExample()
    infiniteStreamExample()
    acquireFileExample()
  }
}