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

import scala.util.Random
import scala.math.floor

/** A map from a keyspace [0,1] to a set of targets [0, numTargets).
 *
 *  A target may be removed from the map (redistributing the target's keys).
 */
class HashRing(numTargets : Int, randomSeed : Int = 0) {

  val random = new Random(randomSeed)

  // Cut the keyspace more finely to handle a number of removals.
  val margin = 20
  val pieces = margin * numTargets * numTargets
  /** An array of pieces values, each representing 1/pieces of the keyspace.
   *
   *  The value of each array cell is a randomly assigned target
   *  (if target has been subsequently removed, then continue to next cell).
   */
  val assignment = random.shuffle( (1 to margin*numTargets).flatMap(_ => 0 until numTargets) )
  assert(assignment.length == pieces)

  // Some versions of Scala do not implement @volatile correctly.
  // https://issues.scala-lang.org/browse/SI-2424
  // @todo Test @volatile.
  @volatile var dropped = List[Int]()

  /** Map from key-range-piece (offset) to an unremoved target [0, numTargets). */ 
  protected def getTarget(offset : Int) : Int = {
    assert(offset >= 0, "offset into key-range assignments must be nonnegative")
    assert(offset < pieces, "offset out of range given key-range-pieces count")
    val target = assignment(offset)
    if (dropped.contains(target))
      getTarget( (offset + 1) % pieces )
    else
      target
  }

  /** Map from key [0.0, 1.0] to an unremoved target [0, numTargets). */
  def apply(key : Float) : Int = {
    assert(key > 0, "key must be between [0.0, 1.0]")
    val offset = floor(key * pieces).toInt
    getTarget(offset % pieces)
  }

  /** Remove a target [0, numTargets) (so apply will never return it again).
   *  Do we need synchronized?
   */
  def remove(target: Int) = synchronized {
    assert(target < numTargets, "cannot remove target number too large for ring")
    dropped = dropped :+ target
  }

}

