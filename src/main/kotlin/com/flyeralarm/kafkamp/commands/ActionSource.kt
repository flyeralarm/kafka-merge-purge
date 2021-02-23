package com.flyeralarm.kafkamp.commands

import org.slf4j.Logger
import java.io.InputStream
import java.util.Scanner

typealias ActionSource = () -> Ask.Action

fun streamActionSource(stream: InputStream, logger: Logger, promptLogger: Logger): ActionSource =
    Scanner(stream).let { scanner ->
        {
            generateSequence {
                promptLogger.info("Would you like to (m)erge, (p)urge or (s)kip the record? (default: merge)")
                return@generateSequence scanner.nextLine().toLowerCase().takeIf { it.isNotEmpty() } ?: "m"
            }.mapNotNull {
                when (it) {
                    "m" -> Ask.Action.MERGE
                    "p" -> Ask.Action.PURGE
                    "s" -> Ask.Action.SKIP
                    else -> {
                        logger.info("Unknown option, try again.")
                        null
                    }
                }
            }.first()
        }
    }
