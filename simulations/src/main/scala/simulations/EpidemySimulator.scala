package simulations

import math.random

class EpidemySimulator extends Simulator {

  def randomBelow(i: Int) = (random * i).toInt

  protected[simulations] object SimConfig {
    val population: Int = 300
    val roomRows: Int = 8
    val roomColumns: Int = 8
    val prevalenceRate: Double = 0.01
    val deathRate: Int = 25
    val transmissibilityRate: Int = 40
  }

  import SimConfig._

  var initialInfected = (population * prevalenceRate).toInt
  val persons: List[Person] = (1 to 300).map(new Person(_)).toList


  class Person (val id: Int) {
    import RoomHelpers._

    private var innerInfected = false
    private var innerSick = false
    private var innerDead = false
    private var innerImmune = false

    infected = if (initialInfected > 0) { initialInfected -= 1; true; } else false

    // demonstrates random number generation
    var row: Int = randomBelow(roomRows)
    var col: Int = randomBelow(roomColumns)

    //
    // to complete with simulation logic
    //

    // Do an initial move.
    action()

    def infected = innerInfected
    def infected_=(newValue: Boolean) {
      if (!dead) {
        val oldValue = innerInfected
        innerInfected = newValue
        if (!oldValue && newValue) {
          // We are newly infected
          afterDelay(6) { sick = true }
        }
      }
    }

    def sick = innerSick
    def sick_=(newValue: Boolean) {
      if (!dead) {
        innerSick = newValue
        if (innerSick) afterDelay(8) {
          dead = rollTheDie(deathRate)
        }
      }
    }

    def dead = innerDead
    def dead_=(newValue: Boolean) {
      // Death cannot be undone.
      if (!dead) {
        innerDead = newValue
        if (!dead && infected) afterDelay(2) {
          sick = false
          immune = true
        }
      }
    }

    def immune = innerImmune
    def immune_=(newValue: Boolean) {
      if (!dead) {
        innerImmune = newValue
        if (immune) afterDelay(2) {
          immune = false
          infected = false
        }
      }
    }

    def action() {
      afterDelay(randomBelow(5) + 1){
        if (!dead) {
          move()
          action()
        }
      }
    }

    def move() = findRoomToMoveTo(row, col) match {
      case Some((r, c)) => moveToRoom(r, c)
      case None => // Zombies everywhere!
    }

    def moveToRoom(r: Int, c: Int) = {
      row = r
      col = c
      if (!infected && !immune && doesRoomContainInfectedPeople(row, col)) {
        infected = rollTheDie(transmissibilityRate)
      }
    }

    def rollTheDie(rate: Int) = (randomBelow(100) + 1) <= rate
  }

  object RoomHelpers {
    import RoomNavigationHelpers._

    def doesRoomContainInfectedPeople(room: (Int, Int)) =
      doesRoomContainPeopleMeetingPredicate(room)(p => p.infected)

    def doesRoomContainVisiblyInfectedPeople(room: (Int, Int)) =
      doesRoomContainPeopleMeetingPredicate(room)(p => p.sick || p.dead)

    def doesRoomContainPeopleMeetingPredicate(room: (Int, Int))(pred: Person => Boolean) =
      persons.filter(p => p.row == room._1 && p.col == room._2).exists(pred)

    val possibleRooms = List[(Int, Int) => (Int, Int)](getNorth, getSouth, getEast, getWest)
    def findRoomToMoveTo(room: (Int, Int)): Option[(Int, Int)] = {
      val rooms = possibleRooms.map(_(room._1, room._2)).filterNot(doesRoomContainVisiblyInfectedPeople)
      if (rooms.isEmpty)
        None
      else
        Some(rooms(randomBelow(rooms.length)))
    }
  }

  object RoomNavigationHelpers {
    def getNorth(r: Int, c: Int): (Int, Int) = {
      (if (r - 1 < 0) roomRows - 1 else r - 1, c)
    }

    def getSouth(r: Int, c: Int): (Int, Int) = {
      ((r + 1) % roomRows, c)
    }

    def getEast(r: Int, c: Int): (Int, Int) = {
      (r, (c + 1) % roomColumns)
    }

    def getWest(r: Int, c: Int): (Int, Int) = {
      (r, if (c - 1 < 0) roomColumns - 1 else c - 1)
    }
  }
}
