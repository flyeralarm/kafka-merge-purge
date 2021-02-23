package com.flyeralarm.kafkamp

import kotlin.test.assertEquals
import kotlin.test.asserter

inline fun <reified T : Throwable> assertThrows(expectedMessage: String): ThrowsAssertion =
    ThrowsAssertion(ThrowsAssertion.AssertionChain(T::class.java, expectedMessage))

inline fun <reified T : Throwable> assertThrows(expectedMessage: String, body: () -> Unit) {
    assertThrows<T>(expectedMessage).test(body)
}

class ThrowsAssertion(val chain: AssertionChain<*>) {
    inline fun <reified T : Throwable> withCause(expectedMessage: String) =
        ThrowsAssertion(chain.addTerminal(AssertionChain(T::class.java, expectedMessage)))

    inline fun test(body: () -> Unit) {
        runCatching(body).fold(
            {
                asserter.fail("Unexpected success, expected <${chain.throwableClass}> to be thrown")
            },
            {
                chain.evaluate(it, 0)
            }
        )
    }

    data class AssertionChain<T : Throwable>(
        val throwableClass: Class<T>,
        val expectedMessage: String,
        val next: AssertionChain<*>? = null
    ) {
        fun evaluate(throwable: Throwable, depth: Int) {
            assertEquals<Class<*>>(throwableClass, throwable.javaClass, "Unexpected exception at level $depth,")

            assertEquals(expectedMessage, throwable.message, "Unexpected exception message at level $depth,")

            if (next == null) {
                return
            }

            if (throwable.cause == null) {
                asserter.fail("Unexpected end of throwable chain, expected <${next.throwableClass}> as cause at level $depth")
            } else {
                next.evaluate(throwable.cause!!, depth + 1)
            }
        }

        fun addTerminal(terminal: AssertionChain<*>): AssertionChain<T> =
            if (next == null) {
                copy(next = terminal)
            } else {
                copy(next = next.addTerminal(terminal))
            }
    }
}
