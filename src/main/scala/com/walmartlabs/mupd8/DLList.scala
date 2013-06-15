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