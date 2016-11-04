package io.getquill.monad

import scala.language.experimental.macros
import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import io.getquill.context.Context

trait IOMonad {
  this: Context[_, _] =>

  sealed trait Effect

  object Effect {
    sealed trait Read extends Effect
    sealed trait Write extends Effect
  }

  def runIO[T](quoted: Quoted[T]): IO[RunQuerySingleResult[T], Effect.Read] = macro IOMonadMacro.runIO
  def runIO[T](quoted: Quoted[Query[T]]): IO[RunQueryResult[T], Effect.Read] = macro IOMonadMacro.runIO
  def runIO(quoted: Quoted[Action[_]]): IO[RunActionResult, Effect.Write] = macro IOMonadMacro.runIO
  def runIO[T](quoted: Quoted[ActionReturning[_, T]]): IO[RunActionReturningResult[T], Effect.Write] = macro IOMonadMacro.runIO
  def runIO(quoted: Quoted[BatchAction[Action[_]]]): IO[RunBatchActionResult, Effect.Write] = macro IOMonadMacro.runIO
  def runIO[T](quoted: Quoted[BatchAction[ActionReturning[_, T]]]): IO[RunBatchActionReturningResult[T], Effect.Write] = macro IOMonadMacro.runIO

  protected case object Unit extends IO[Unit, Effect]
  protected case class Run[T, E <: Effect](f: () => Result[T]) extends IO[T, E]
  protected case class Sequence[A, M[X] <: TraversableOnce[X], E <: Effect](in: M[IO[A, E]], cbf: CanBuildFrom[M[A], A, M[A]]) extends IO[M[A], E]
  protected case class TransformWith[T, S, E1 <: Effect, E2 <: Effect](io: IO[T, E1], f: Try[T] => IO[S, E2]) extends IO[S, E1 with E2]

  def unsafePerformIO[T](io: IO[T, _]): Result[T]

  object IO {

    def unit: IO[Unit, Effect] = Unit

    def sequence[A, M[X] <: TraversableOnce[X], E <: Effect](in: M[IO[A, E]])(implicit cbf: CanBuildFrom[M[A], A, M[A]]): IO[M[A], E] = Sequence(in, cbf)

    def transformWith[T, S, E1 <: Effect, E2 <: Effect](io: IO[T, E1])(f: Try[T] => IO[S, E2]): IO[S, E1 with E2] = TransformWith(io, f)

    def transform[T, S, E <: Effect](io: IO[T, E])(f: Try[T] => Try[S]): IO[S, E] =
      transformWith(io) { r =>
        successful(f(r)).lowerFromTry
      }

    def zip[T, E1 <: Effect, S, E2 <: Effect](a: IO[T, E1], b: IO[S, E2]): IO[(T, S), E1 with E2] =
      sequence(List(a, b)).map {
        case a :: b :: Nil => (a.asInstanceOf[T], b.asInstanceOf[S])
        case other => throw new IllegalStateException("Sequence returned less than two elements")
      }

    def failed[T](exception: Throwable): IO[T, Effect] = apply(throw exception)

    def successful[T](result: T): IO[T, Effect] = apply(result)

    def fromTry[T](result: Try[T]): IO[T, Effect] = unit.transform(_ => result)

    def apply[T](body: => T): IO[T, Effect] = unit.map(_ => body)

    def find[T, E <: Effect](ios: collection.immutable.Iterable[IO[T, E]])(p: T => Boolean): IO[Option[T], E] =
      sequence(ios).map(_.find(p))

    def foldLeft[T, R, E <: Effect](ios: collection.immutable.Iterable[IO[T, E]])(zero: R)(op: (R, T) => R): IO[R, E] =
      sequence(ios).map(_.foldLeft(zero)(op))

    def reduceLeft[T, R >: T, E <: Effect](ios: collection.immutable.Iterable[IO[T, E]])(op: (R, T) => R): IO[R, E] =
      sequence(ios).map(_.reduceLeft(op))

    def traverse[A, B, M[X] <: TraversableOnce[X], E <: Effect](in: M[A])(fn: A => IO[B, E])(implicit cbf: CanBuildFrom[M[A], B, M[B]]): IO[M[B], E] =
      sequence(in.map(fn)).map(r => cbf().++=(r).result)
  }

  sealed trait IO[+T, -E <: Effect] {

    def zip[S, E2 <: Effect](that: IO[S, E2]): IO[(T, S), E with E2] = IO.zip(this, that)
    def transform[S](f: Try[T] => Try[S]): IO[S, E] = IO.transform(this)(f)
    def transformWith[S, E2 <: Effect](f: Try[T] => IO[S, E2]): IO[S, E with E2] = IO.transformWith(this)(f)

    def lowerFromTry[U](implicit ev: T => Try[U]) =
      map(ev).flatMap {
        case Success(v) => IO.successful(v)
        case Failure(e) => IO.failed(e)
      }

    def liftToTry: IO[Try[T], E] =
      transformWith(IO.successful)

    def failed: IO[Throwable, E] =
      transform {
        case Failure(t) => Success(t)
        case Success(v) => Failure(new NoSuchElementException("IO.failed not completed with a throwable."))
      }

    def transform[S](s: T => S, f: Throwable => Throwable): IO[S, E] =
      transform {
        case Success(r) => Try(s(r))
        case Failure(t) => Try(throw f(t)) // will throw fatal errors!
      }

    def map[S](f: T => S): IO[S, E] = transform(_.map(f))

    def flatMap[S, E2 <: Effect](f: T => IO[S, E2]): IO[S, E with E2] = transformWith {
      case Success(s) => f(s)
      case Failure(_) => this.asInstanceOf[IO[S, E with E2]]
    }

    def filter(p: T => Boolean): IO[T, E] =
      map { r => if (p(r)) r else throw new NoSuchElementException("IO.filter predicate is not satisfied") }

    final def withFilter(p: T => Boolean): IO[T, E] = filter(p)

    def collect[S](pf: PartialFunction[T, S]): IO[S, E] =
      map {
        r => pf.applyOrElse(r, (t: T) => throw new NoSuchElementException("IO.collect partial function is not defined at: " + t))
      }

    def recover[U >: T](pf: PartialFunction[Throwable, U]): IO[U, E] =
      transform { _ recover pf }

    def recoverWith[U >: T, E2 <: Effect](pf: PartialFunction[Throwable, IO[U, E2]]): IO[U, E with E2] =
      transformWith {
        case Failure(t) => pf.applyOrElse(t, (_: Throwable) => this)
        case Success(_) => this
      }

    def zipWith[U, R, E2 <: Effect](that: IO[U, E2])(f: (T, U) => R): IO[R, E with E2] =
      zip(that).map(f.tupled)

    def fallbackTo[U >: T, E2 <: Effect](that: IO[U, E2]): IO[U, E with E2] =
      if (this eq that) this
      else recoverWith { case _ => that } recoverWith { case _ => this }

    def andThen[U](pf: PartialFunction[Try[T], U]): IO[T, E] =
      transform {
        result =>
          pf.applyOrElse[Try[T], Any](result, Predef.identity[Try[T]])
          result
      }
  }
}
