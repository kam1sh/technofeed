package org.notahabr.subsystem

import com.zaxxer.hikari.HikariDataSource
import io.ktor.server.application.*
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.flywaydb.core.Flyway
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import org.koin.core.context.loadKoinModules
import org.koin.core.qualifier.named
import org.koin.dsl.module
import java.util.concurrent.Executors

fun databaseModule(
    url: String,
    username: String,
    password: String,
) = module {
    single(named("primary")) {
        val ds = HikariDataSource()
        ds.jdbcUrl = url
        ds.username = username
        ds.password = password
        val migrator = Flyway
            .configure()
            .dataSource(ds)
            .locations("classpath:flyway")
            .load()
        migrator.repair()
        migrator.migrate()
        Database.connect(ds)
    }
    single {
        PrimaryDatabase(
            db = get(named("primary")),
            executor = Executors.newFixedThreadPool(10).asCoroutineDispatcher()
        )
    }
}


class PrimaryDatabase(
    val db: Database,
    val executor: ExecutorCoroutineDispatcher
) {
    suspend fun <T> transaction(statement: suspend Transaction.() -> T): T {
        return newSuspendedTransaction(executor, db, statement = statement)
    }
}



fun Application.dbModule() {
    val mod = databaseModule(
        url = environment.config.property("database.url").getString(),
        username = environment.config.property("database.user").getString(),
        password = environment.config.property("database.password").getString()
    )
    loadKoinModules(mod)
}