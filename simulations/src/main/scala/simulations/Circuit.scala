package simulations

import common._

class Wire {
  private var sigVal = false
  private var actions: List[Simulator#Action] = List()

  def getSignal: Boolean = sigVal
  
  def setSignal(s: Boolean) {
    if (s != sigVal) {
      sigVal = s
      actions.foreach(action => action())
    }
  }

  def addAction(a: Simulator#Action) {
    actions = a :: actions
    a()
  }
}

abstract class CircuitSimulator extends Simulator {

  val InverterDelay: Int
  val AndGateDelay: Int
  val OrGateDelay: Int

  def probe(name: String, wire: Wire) {
    wire addAction {
      () => afterDelay(0) {
        println(
          "  " + currentTime + ": " + name + " -> " +  wire.getSignal)
      }
    }
  }

  def inverter(input: Wire, output: Wire) {
    def invertAction() {
      val inputSig = input.getSignal
      afterDelay(InverterDelay) { output.setSignal(!inputSig) }
    }
    input addAction invertAction
  }

  def andGate(a1: Wire, a2: Wire, output: Wire) {
    def andAction() {
      val a1Sig = a1.getSignal
      val a2Sig = a2.getSignal
      afterDelay(AndGateDelay) { output.setSignal(a1Sig & a2Sig) }
    }
    a1 addAction andAction
    a2 addAction andAction
  }

  //
  // to complete with orGates and demux...
  //

  def orGate(a1: Wire, a2: Wire, output: Wire) {
    def orAction() {
      val a1Sig = a1.getSignal
      val a2Sig = a2.getSignal
      afterDelay(OrGateDelay) { output.setSignal(a1Sig | a2Sig) }
    }
    a1 addAction orAction
    a2 addAction orAction
  }
  
  def orGate2(a1: Wire, a2: Wire, output: Wire) {
    val notIn1, notIn2, notOut = new Wire
    inverter(a1, notIn1)
    inverter(a2, notIn2)
    andGate(notIn1, notIn2, notOut)
    inverter(notOut, output)
  }

  def noOp(in: Wire, out: Wire) {
    def noOpAction() {
      out.setSignal(in.getSignal)
    }
    in addAction noOpAction
  }

  def demux(in: Wire, c: List[Wire], out: List[Wire]): Unit = (c, out) match {
    case (Nil, o::Nil) => {
      noOp(in, o)
    }
    case (cont::conts, os) =>
    {
      val a, b = new Wire()
      demux(in, cont, a, b)

      demux(a, conts, os.take(os.length / 2))
      demux(b, conts, os.drop(os.length / 2))
    }
    case (_, _) =>
  }

  def demux(in: Wire, c: Wire, out1: Wire, out2: Wire) = {
    val cNot = new Wire
    andGate(in, c, out1)
    inverter(c, cNot)
    andGate(in, cNot, out2)
  }
}

object Circuit extends CircuitSimulator {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5

  def andGateExample {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    probe("in1", in1)
    probe("in2", in2)
    probe("out", out)
    in1.setSignal(false)
    in2.setSignal(false)
    run

    in1.setSignal(true)
    run

    in2.setSignal(true)
    run
  }

  //
  // to complete with orGateExample and demuxExample...
  //
}

object CircuitMain extends App {
  // You can write tests either here, or better in the test class CircuitSuite.
  Circuit.andGateExample
}
