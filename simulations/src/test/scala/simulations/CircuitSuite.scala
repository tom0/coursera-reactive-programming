package simulations

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CircuitSuite extends CircuitSimulator with FunSuite {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5
  
  test("andGate example") {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run
    
    assert(out.getSignal === false, "and 1")

    in1.setSignal(true)
    run
    
    assert(out.getSignal === false, "and 2")

    in2.setSignal(true)
    run
    
    assert(out.getSignal === true, "and 3")
  }

  //
  // to complete with tests for orGate, demux, ...
  //
  test("orGate") {
    testOrGate(orGate)
  }

  test("orGate2") {
    testOrGate(orGate2)
  }

  def testOrGate(orGate: (Wire, Wire, Wire) => Unit) {
    val in1, in2, out = new Wire
    orGate(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run

    assert(out.getSignal === false, "or 1")

    in1.setSignal(true)
    run

    assert(out.getSignal === true, "or 2")

    in2.setSignal(true)
    run

    assert(out.getSignal === true, "or 3")

    in1.setSignal(false)
    run

    assert(out.getSignal === true, "or 4")
  }

  test("demux1") {
    // The most basic case: 1 output wire, no control wires.

    val in, out = new Wire
    demux(in, List(), List(out))
    in.setSignal(true)
    run

    assert(out.getSignal === true, "demux1 1")

    in.setSignal(false)
    run

    assert(out.getSignal === false, "demux1 2")
  }

  test("demux2") {
//    1 control wire, 2 output wires.

    val in, out1, out2, c = new Wire
    demux(in, List(c), List(out1, out2))
    in.setSignal(true)
    c.setSignal(true)
    run

    assert(out1.getSignal === true, "demux2 1")
    assert(out2.getSignal === false, "demux2 2")

    c.setSignal(false)
    run

    assert(out1.getSignal === false, "demux2 3")
    assert(out2.getSignal === true, "demux2 4")
  }

//  def testDemux(controlWireCount: Int) {
//    def findOutWireIdx(cIdx: Int, cSet: Boolean) = {
//      (cIdx * 2) + (if (cSet) 0 else 1)
//    }
//
//    val cs = List.fill(controlWireCount)(new Wire)
//    val os = List.fill(math.pow(2, controlWireCount).toInt)(new Wire)
//    val in = new Wire
//    demux(in, cs, os)
//    in.setSignal(true)
//
//    for ((c, i) <- cs.view.zipWithIndex) {
//      c.setSignal(true)
//      run
//      assert(os(findOutWireIdx(i, c.getSignal)).getSignal === true, s"testDemux 1. controlWireCount = ${controlWireCount}")
//
//      c.setSignal(false)
//      run
//      assert(os(findOutWireIdx(i, c.getSignal)).getSignal === true, s"testDemux 2. controlWireCount = ${controlWireCount}")
//    }
//  }
}


