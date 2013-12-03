package suggestions
package observablex

import scala.concurrent.{Future, ExecutionContext}
import scala.util._
import scala.util.Success
import scala.util.Failure
import java.lang.Throwable
import rx.lang.scala.Observable
import rx.lang.scala.Scheduler
import rx.lang.scala.subscriptions.Subscription
import rx.lang.scala.subjects.ReplaySubject

object ObservableEx {

  /** Returns an observable stream of values produced by the given future.
   * If the future fails, the observable will fail as well.
   *
   * @param f future whose values end up in the resulting observable
   * @return an observable completed after producing the value of the future, or with an exception
   */
  def apply[T](f: Future[T])(implicit execContext: ExecutionContext): Observable[T] = {
    val subject = ReplaySubject[T]()

    f onComplete {
      case Success(value) => {
        subject.onNext(value)
        subject.onCompleted()
      }
      case Failure(ex) => {
        subject.onError(ex)
      }
    }

    subject
  }

// TODO: Will be interesting to see if this would have worked...
//  def apply[T](f: Future[T])(implicit execContext: ExecutionContext): Observable[T] = Observable { observer =>
//    val s = Subscription()
//    f.filter(_ => !s.isUnsubscribed) onComplete {
//      case Success(t) => {
//        observer.onNext(t)
//        observer.onCompleted()
//      }
//      case Failure(ex) => observer.onError(ex)
//    }
//    s
//  }

}