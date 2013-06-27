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

class Node[T](val item: T, var next: Node[T] = null, var prev: Node[T] = null)

class DLList[T] {
  var front: Node[T] = null
  var last: Node[T] = null
  final def add(node: Node[T]) {
    assert(node.next == null && node.prev == null)
    if (front != null) front.prev = node
    node.next = front
    front = node
    if (last == null) {
      last = front
      assert(front.next == null)
    }
  }

  final def remove(node: Node[T]) {
    if (node.prev == null) {
      assert(node == front)
      front = node.next
    } else {
      node.prev.next = node.next
    }
    if (node.next == null) {
      assert(node == last)
      last = node.prev
    } else {
      node.next.prev = node.prev
    }
    node.prev = null
    node.next = null
  }
}