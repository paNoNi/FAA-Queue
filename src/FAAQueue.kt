import kotlinx.atomicfu.*

class FAAQueue<T> {
    private val head: AtomicRef<Segment<T>> // Head pointer, similarly to the Michael-Scott queue (but the first node is _not_ sentinel)
    private val tail: AtomicRef<Segment<T>> // Tail pointer, similarly to the Michael-Scott queue

    init {
        val firstNode = Segment<T>()
        head = atomic(firstNode)
        tail = atomic(firstNode)
    }

    /**
     * Adds the specified element [x] to the queue.
     */
    fun enqueue(x: T) {
        while (true) {
            val tail = this.tail.value
            val enqIdx: Int = tail.enqIdx.getAndIncrement()
            if (enqIdx >= SEGMENT_SIZE) {
                val newSegment = Segment(x)
                if (tail.next.compareAndSet(null, newSegment)) {
                    this.tail.getAndSet(newSegment)
                    break
                }
                this.tail.compareAndSet(tail, tail.next.value!!)
            } else if (tail.elements[enqIdx].compareAndSet(null, x)) break
        }
    }

    /**
     * Retrieves the first element from the queue
     * and returns it; returns `null` if the queue
     * is empty.
     */
    fun dequeue(): T? {
        while (true) {
            val head = this.head.value
            val deqIdx = head.deqIdx.getAndIncrement()
            if (deqIdx >= SEGMENT_SIZE) {
                val headNext: Segment<T> = head.next.value ?: return null
                this.head.compareAndSet(head, headNext)
                continue
            }
            return head.elements[deqIdx].getAndSet(DONE as T) ?: continue
        }
    }

    /**
     * Returns `true` if this queue is empty;
     * `false` otherwise.
     */
    val isEmpty: Boolean get() {
        while (true) {
            val head: Segment<T> = head.value
            if (head.isEmpty) {
                val headNext: Segment<T> = head.next.value ?: return true
                this.head.compareAndSet(head, headNext)
                continue
            } else return false
        }
    }
}

private class Segment<T> {
    val next: AtomicRef<Segment<T>?> = atomic(null)
    val enqIdx = atomic(0) // index for the next enqueue operation
    val deqIdx = atomic(0) // index for the next dequeue operation
    val elements = atomicArrayOfNulls<T>(SEGMENT_SIZE)

    constructor() // for the first segment creation

    constructor(x: T?) { // each next new segment should be constructed with an element
        enqIdx.getAndSet(1)
        elements[0].getAndSet(x)
    }

    val isEmpty: Boolean get() = deqIdx.value >= enqIdx.value || deqIdx.value >= SEGMENT_SIZE

}

private val DONE = Any() // Marker for the "DONE" slot state; to avoid memory leaks
const val SEGMENT_SIZE = 2 // DO NOT CHANGE, IMPORTANT FOR TESTS