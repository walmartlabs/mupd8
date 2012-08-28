/**
 * Copyright 2011-2012 @WalmartLabs, a division of Wal-Mart Stores, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.walmartlabs.mupd8

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
// does not exist until ScalaTest 1.6.1 (for Scala 2.9)
// import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.ShouldMatchers

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class HashRingTest extends FunSuite with ShouldMatchers {

  val random = new Random

/* Not available until ScalaTest 1.6.1 for Scala 2.9:
  before {
    random.setSeed(0)
  }
*/

  def checkRing(ring : HashRing, maximum : Int, vacated : List[Int]) = {
    val random = new Random(0)
    val expectedCount = 50
    val numTrials = (maximum - vacated.size) * expectedCount
    var histogram = Array.fill(maximum)(0)
    (1 to numTrials).foreach { _ =>
      val k = random.nextFloat()
      val v = ring(k)
      v should be >= (0)
      v should be < (maximum)
      vacated should not contain (v)
      histogram(v) += 1
    }
    // TODO a real analysis of the distribution to sanity-check the outcomes
    histogram.zipWithIndex.foreach{ pair =>
      val count = pair._1
      val target = pair._2
      if (vacated.contains(target))
        assert(count == 0, "Target "+target+" got "+count+" assignments, not zero.")
      else {
        assert(count >= expectedCount/2, "Target "+target+" got "+count+" assignments, less than half the expected count "+expectedCount);
        assert(count < 2*expectedCount, "Target "+target+" got "+count+" assignments, more than twice the expected count "+expectedCount);
      }
      // println("Target "+target+" got "+count+" assignments.")
    }
  }

  test("simple ring of 10 targets") {
    val targets = 10
    val ring = new HashRing(targets)

    val vacated = List[Int]()
    checkRing(ring, targets, vacated)
  }

  test("simple ring with removals") {
    val targets = 5
    val ring = new HashRing(targets)

    val removals = List[Int](4, 0, 2, 3)
    // List.inits requires a newer version of Scala after 2.8:
    /*
    removals.inits.reverse.tail.foreach { removed =>
      ring.remove(removed.tail)
      checkRing(ring, targets, removed)
    }
    */
    // Check ring after each individual removal in removals.
    removals.foldLeft(List[Int]()) { (vacated, removal) =>
      ring.remove(removal)
      val vacatedWithRemoval = vacated :+ removal

      checkRing(ring, targets, vacatedWithRemoval)
      vacatedWithRemoval
    }
  }

}

