package com.flyeralarm.kafkamp.commands

import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.slf4j.Logger
import kotlin.test.Test
import kotlin.test.assertEquals

class ActionSourceTest {
    @Test
    fun `prompts user to enter character`() {
        val promptLogger = mockk<Logger>(relaxed = true)

        "m\n".byteInputStream().use {
            streamActionSource(it, mockk(relaxed = true), promptLogger)()

            verify(exactly = 1) {
                promptLogger.info("Would you like to (m)erge, (p)urge or (s)kip the record? (default: merge)")
            }
        }
    }

    @Test
    fun `maps 'm' to merge action`() {
        for (c in listOf("m", "M")) {
            "$c\n".byteInputStream().use {
                assertEquals(Ask.Action.MERGE, streamActionSource(it, mockk(relaxed = true), mockk(relaxed = true))())
            }
        }
    }

    @Test
    fun `maps empty line to merge action`() {
        "\n".byteInputStream().use {
            assertEquals(Ask.Action.MERGE, streamActionSource(it, mockk(relaxed = true), mockk(relaxed = true))())
        }
    }

    @Test
    fun `maps 'p' to purge action`() {
        for (c in listOf("p", "P")) {
            "$c\n".byteInputStream().use {
                assertEquals(Ask.Action.PURGE, streamActionSource(it, mockk(relaxed = true), mockk(relaxed = true))())
            }
        }
    }

    @Test
    fun `maps 's to skip action`() {
        for (c in listOf("s", "S")) {
            "$c\n".byteInputStream().use {
                assertEquals(Ask.Action.SKIP, streamActionSource(it, mockk(relaxed = true), mockk(relaxed = true))())
            }
        }
    }

    @Test
    fun `prompts again for unknown character`() {
        val logger = mockk<Logger>(relaxed = true)
        val promptLogger = mockk<Logger>(relaxed = true)

        "x\nm\n".byteInputStream().use {
            assertEquals(Ask.Action.MERGE, streamActionSource(it, logger, promptLogger)())

            verifyOrder {
                promptLogger.info("Would you like to (m)erge, (p)urge or (s)kip the record? (default: merge)")
                logger.info("Unknown option, try again.")
                promptLogger.info("Would you like to (m)erge, (p)urge or (s)kip the record? (default: merge)")
            }
        }
    }
}
