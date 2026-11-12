package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        const val PROCESSING_TIME = 2.0
        const val INBOUND_RATE_FACTOR = 0.9
        const val RETRY_AFTER_MILLIS = 100L
        const val MAX_ATTEMPTS = 4
        const val BASE_BACKOFF_MILLIS = 50L
        const val CLIENT_TIMEOUT_MARGIN_MILLIS = 100L
        const val RATE_LIMIT_WINDOW_MARGIN_MILLIS = 100L

        val RETRIABLE_CODES = setOf(408, 429, 500, 502, 503, 504)
    }

    private class AttemptOutcome(val retryAfterMillis: Long?)

    override fun tryAccept(deadline: Long): Long? {
        val waitSeconds = (deadline - now() - processingBudgetMillis) / 1000
        if (waitSeconds >= 0 && inboundRateLimiter.tryAcquire(inboundRate * (waitSeconds + 1))) return null
        return now() + RETRY_AFTER_MILLIS
    }

    override fun releaseAccepted() {
        inboundRateLimiter.leak()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofMillis(1000 + RATE_LIMIT_WINDOW_MARGIN_MILLIS),
    )

    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val outgoingQueue = ReentrantLock(true)

    private val inboundRate = rateLimitPerSec * INBOUND_RATE_FACTOR

    private val inboundRateLimiter = LeakingBucketRateLimiter()

    private val processingBudgetMillis = (requestAverageProcessingTime.toMillis() * PROCESSING_TIME).toLong()

    private val outgoingRequests = Counter.builder("shop.provider.requests")
        .tag("account", accountName)
        .register(meterRegistry)

    private val droppedPayments = Counter.builder("shop.payments.dropped")
        .tag("account", accountName)
        .register(meterRegistry)

    private val retriedRequests = Counter.builder("shop.provider.retries")
        .tag("account", accountName)
        .register(meterRegistry)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        var accepted = true
        try {
            var attempt = 0
            while (true) {
                if (attempt > 0 && deadline - now() < requestAverageProcessingTime.toMillis()) {
                    logger.warn("[$accountName] No time left to retry payment $paymentId after $attempt attempts")
                    return
                }

                val transactionId = UUID.randomUUID()

                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }

                logger.info("[$accountName] Submit: $paymentId , txId: $transactionId, attempt: ${attempt + 1}")

                if (deadline - now() < requestAverageProcessingTime.toMillis()) {
                    droppedPayments.increment()
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline cannot be met.")
                    }
                    return
                }

                outgoingQueue.withLock { rateLimiter.tickBlocking() }
                if (accepted) {
                    inboundRateLimiter.leak()
                    accepted = false
                }

                val outcome = executeAttempt(paymentId, transactionId, amount, deadline)
                attempt++
                val retryAfter = outcome.retryAfterMillis ?: return

                if (attempt >= MAX_ATTEMPTS) {
                    logger.warn("[$accountName] Retry limit ($MAX_ATTEMPTS) reached for payment $paymentId")
                    return
                }

                val delay = maxOf(retryAfter, BASE_BACKOFF_MILLIS shl (attempt - 1))
                if (now() + delay + requestAverageProcessingTime.toMillis() > deadline) {
                    logger.warn("[$accountName] No time left to retry payment $paymentId after $attempt attempts")
                    return
                }

                retriedRequests.increment()
                Thread.sleep(delay)
            }
        } finally {
            if (accepted) inboundRateLimiter.leak()
        }
    }

    private fun executeAttempt(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long): AttemptOutcome {
        ongoingWindow.acquire()
        try {
            val timeLeft = deadline - now()
            if (timeLeft <= 0) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded before request.")
                }
                return AttemptOutcome(null)
            }

            outgoingRequests.increment()

            val providerTimeout = Duration.ofMillis(timeLeft)

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=$providerTimeout")
                post(emptyBody)
            }.build()

            val call = client.newCall(request)
            call.timeout().timeout(timeLeft + CLIENT_TIMEOUT_MARGIN_MILLIS, TimeUnit.MILLISECONDS)

            call.execute().use { response ->
                val rawBody = response.body?.string()
                val body = try {
                    mapper.readValue(rawBody, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $rawBody")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                if (body.result) return AttemptOutcome(null)

                return when {
                    response.code == 200 -> AttemptOutcome(0)
                    response.code == 429 -> AttemptOutcome(parseRetryAfterMillis(response.header("Retry-After")))
                    response.code in RETRIABLE_CODES -> AttemptOutcome(0)
                    else -> AttemptOutcome(null)
                }
            }
        } catch (e: InterruptedIOException) {
            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
            return AttemptOutcome(0)
        } catch (e: IOException) {
            logger.error("[$accountName] Payment failed (IO) for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
            return AttemptOutcome(0)
        } catch (e: Exception) {
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
            return AttemptOutcome(null)
        } finally {
            ongoingWindow.release()
        }
    }

    private fun parseRetryAfterMillis(header: String?): Long =
        header?.trim()?.toDoubleOrNull()?.let { (it * 1000).toLong() } ?: 0L

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()