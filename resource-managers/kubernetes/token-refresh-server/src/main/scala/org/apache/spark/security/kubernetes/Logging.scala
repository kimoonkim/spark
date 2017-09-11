package org.apache.spark.security.kubernetes

import org.apache.log4j.{LogManager, Logger, Priority}

trait Logging {

  private var log : Logger = LogManager.getLogger(this.getClass)

  protected def logDebug(msg: => String) = if (log.isDebugEnabled) log.debug(msg)

  protected def logInfo(msg: => String) = if (log.isInfoEnabled) log.info(msg)

  protected def logWarning(msg: => String) = if (log.isEnabledFor(Priority.WARN)) log.warn(msg)

  protected def logWarning(msg: => String, throwable: Throwable) =
    if (log.isEnabledFor(Priority.WARN)) log.warn(msg, throwable)

  protected def logError(msg: => String) = if (log.isEnabledFor(Priority.ERROR)) log.error(msg)
}
