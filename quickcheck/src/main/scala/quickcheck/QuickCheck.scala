package quickcheck

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._
import scala.annotation.tailrec

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  // These first two are actually made unnecessary by the order checking property (gen2) below.
  property("min1") = forAll { a: A =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min2") = forAll { (a: A, b: A) =>
    val h = insert(b, insert(a, empty))
    findMin(h) == (if (a < b) a else b)
  }

  property("delete1") = forAll { a: A =>
    val h = deleteMin(insert(a, empty))
    isEmpty(h)
  }

  property("delete2") = forAll { (a: A, b: A) =>
    val h = deleteMin(insert(b, insert(a, empty)))
    findMin(h) == (if (a < b) b else a)
  }

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("gen2") = forAll { (h: H) =>
    val maybeSorted = toMaybeSortedList(h)
    maybeSorted == maybeSorted.sorted(ord)
  }

  property("meld1") = isEmpty(meld(empty, empty))

  property("meld2") = forAll { (h1: H, h2: H) =>
    val meldedMin = findMin(meld(h1, h2))
    meldedMin == findMin(h1) || meldedMin == findMin(h2)
  }

  property("meld3") = forAll { (h1: H, h2: H) =>
    val meldedMaybeSorted = toMaybeSortedList(meld(h1, h2))
    meldedMaybeSorted == meldedMaybeSorted.sorted(ord)
  }

  property("meld4") = forAll { (a: H, b: H) =>
    val melded = meld(a, b)
    contains(melded, a) && contains(melded, b)
  }

  @tailrec
  final def contains(heap: H, subsetHeap: H): Boolean = (heap, subsetHeap) match {
    case (h, s) if isEmpty(s) => true
    case (h, s) if isEmpty(h) && !isEmpty(s) => false
    case (h, s) if findMin(h) == findMin(s) => contains(deleteMin(h), deleteMin(s))
    case (h, s) if findMin(h) < findMin(s) => contains(h, deleteMin(s))
    case (h, s) if findMin(h) > findMin(s) => contains(deleteMin(h), s)
  }

  @tailrec
  final def toMaybeSortedList(heap: H, acc: List[A] = Nil): List[A] =
    heap match {
      case h if isEmpty(h) => acc
      case h =>
      {
        val min = findMin(h)
        toMaybeSortedList(deleteMin(h), acc :+ min)
      }
    }

  lazy val genHeap: Gen[H] = for {
    i <- arbitrary[A]
    m <- oneOf(value(empty), genHeap)
  } yield insert(i, m)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)
}
