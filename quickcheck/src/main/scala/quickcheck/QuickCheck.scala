package quickcheck

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._
import scala.annotation.tailrec

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min2") = forAll { (a: Int, b: Int) =>
    val h = insert(b, insert(a, empty))
    findMin(h) == (if (a < b) a else b)
  }

//  property("minN") = forAll { a: Int =>
//  }

  property("delete1") = forAll { a: Int =>
    val h = deleteMin(insert(a, empty))
    isEmpty(h)
  }

  property("delete2") = forAll { (a: Int, b: Int) =>
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

  def toMaybeSortedList(heap: H) = {
    @tailrec
    def toMaybeSortedListInner(heap: H, acc: List[A] = Nil): List[A] = {
      println(heap)
      heap match {
        case h if isEmpty(h) => acc
        case h =>
        {
          val min = findMin(h)
          toMaybeSortedListInner(deleteMin(h), acc :+ min)
        }
      }
    }

    val res = toMaybeSortedListInner(heap)
    println("Sorted = " + res)
    res
  }

  lazy val genHeap: Gen[H] = for {
    i <- arbitrary[A]
    m <- oneOf(value(empty), genHeap)
  } yield insert(i, m)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
