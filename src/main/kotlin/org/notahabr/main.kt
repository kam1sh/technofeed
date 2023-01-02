package org.notahabr

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addFileSource
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.callloging.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import org.koin.core.context.startKoin
import org.koin.core.qualifier.named
import org.koin.dsl.module
import org.koin.ktor.ext.get
import org.koin.ktor.plugin.koin
import org.notahabr.core.ArticleFacade
import org.slf4j.LoggerFactory


data class Config(
    val loggers: List<LoggerConfig>
)

data class LoggerConfig(
    val name: String,
    val level: String
)


fun Application.setup(testing: Boolean = false) {
    val lc = LoggerFactory.getILoggerFactory() as LoggerContext
    lc.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).level = Level.WARN

    val config = ConfigLoaderBuilder.default()
        .addFileSource(System.getenv("APP_CONFIG") ?: error("Could not find APP_CONFIG"))
        .strict()
        .build()
        .loadConfigOrThrow<Config>()

    config.loggers.reversed().forEach {
        lc.getLogger(it.name).level = Level.valueOf(it.level)
    }

    install(CallLogging) {
        level = org.slf4j.event.Level.WARN
    }

//    install(StatusPages) {
//        exception<Throwable> { call, throwable ->
//            call.application.log.error("Exception:", throwable)
//        }
//    }

    koin {
        modules(modules = module {
            single { config }
            single(named("feeds")) {
                HttpClient(CIO) {
                    engine {
                        requestTimeout = 10000
                    }
                    install(UserAgent) {
                        agent = "IT-Feeds/1.0"
                    }
                }
            }
        })
    }
}

fun Application.facades() {
    koin {
        modules(module {
            single { ArticleFacade(get()) }
        })
    }
}

fun Application.routes(testing: Boolean = false) {
    routing {
        val articles: ArticleFacade = get()
        get("/") {
            val query = call.request.queryParameters["query"]
            if (query == null) {
                call.respond(HttpStatusCode.BadRequest, "no query provided")
                return@get
            }
            val out = articles.searchBy(query = query)
                .map { it.title }
                .joinToString("\n", postfix = "\n")
            call.respond(out)
        }
    }
}


fun main(args: Array<String>) = EngineMain.main(args)
