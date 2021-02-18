@file:JvmName("KafkaMergePurge")

package com.flyeralarm.kafkamp

import org.slf4j.LoggerFactory
import picocli.CommandLine
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val options = CLI()
    val populateCommandLine = CommandLine(options).setUnmatchedArgumentsAllowed(true)
    try {
        populateCommandLine.parseArgs(*args)
    } catch (exception: CommandLine.ParameterException) {
        exitProcess(populateCommandLine.parameterExceptionHandler.handleParseException(exception, args))
    }
    val factory = Factory(
        options,
        LoggerFactory.getLogger("cli"),
        LoggerFactory.getLogger("prompt")
    )

    exitProcess(factory.commandLine.execute(*args))
}
