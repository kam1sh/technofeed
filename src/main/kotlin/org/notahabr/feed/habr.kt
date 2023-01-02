package org.notahabr.feed

import com.rometools.rome.feed.synd.SyndEntry
import com.rometools.rome.io.SyndFeedInput
import com.rometools.rome.io.XmlReader
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.server.application.*
import io.ktor.server.application.hooks.*
import io.ktor.utils.io.jvm.javaio.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.time.delay
import kotlinx.serialization.json.*
import org.jetbrains.exposed.dao.id.LongIdTable
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.insertAndGetId
import org.jetbrains.exposed.sql.select
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.koin.core.context.loadKoinModules
import org.koin.core.qualifier.named
import org.koin.dsl.module
import org.koin.ktor.ext.get
import org.notahabr.core.tables.ArticlesTable
import org.notahabr.logger
import org.notahabr.subsystem.PrimaryDatabase
import org.notahabr.subsystem.textArray
import java.io.IOException
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

data class BaseArtcle(
    val id: Long,
    val title: String,
    val source: String,
    val link: String,
    val ts: LocalDateTime,
    val previewText: String,
    val previewPic: String
)

data class HabrArticle(
    val base: BaseArtcle,
    val author: HabrAuthor,
    val hubs: List<String>,
    val tags: List<String>,
)

data class HabrAuthor(
    val name: String,
    val link: String,
    val karma: Int,
    val rating: Int
)

object HabrPosts : LongIdTable("habr_posts") {
    val article = reference("article_id", ArticlesTable)
    val tags = textArray("tags", 256)
    val hubs = textArray("hubs", 256)
    val authorName = varchar("author_name", 32)
    val authorLink = text("author_link")
    val authorKarma = integer("author_karma")
    val authorRating = integer("author_rating")
}

class HabrService(
    val client: HttpClient
) {
    val log = logger()

    val table = ArticlesTable

    data class RSSDocument(
        val title: String,
        val link: String,
        val previewText: String,
        val author: String,
        val dt: LocalDateTime,
    )

    suspend fun fetchPage() = flow<SyndEntry> {
        client.prepareGet("https://habr.com/ru/rss/all/all/?fl=ru&limit=50").execute { resp ->
            val stream = resp.bodyAsChannel().toInputStream()
            val input = SyndFeedInput()
            val feed = input.build(XmlReader(stream))
            log.debug(feed.title)
            log.debug(feed.description)
            log.debug("Found {} posts", feed.entries.size)
            feed.entries.forEach { emit(it) }
        }
    }

    fun save(post: HabrArticle) {
        log.info("""
            Title: ${post.base.title}
            author: ${post.author}
            Hubs: ${post.hubs}
            Tags: ${post.tags}
        """.trimIndent())
        val id = table.insertAndGetId {
            it[title] = post.base.title
            it[sourceCol] = post.base.source
            it[link] = post.base.link
            it[ts] = post.base.ts
            it[previewText] = post.base.previewText
            it[previewPic] = post.base.previewPic
        }
        HabrPosts.insert {
            it[article] = id
            it[tags] = post.tags.toTypedArray()
            it[hubs] = post.hubs.toTypedArray()
            it[authorName] = post.author.name
            it[authorLink] = post.author.link
            it[authorRating] = post.author.rating
            it[authorKarma] = post.author.karma
        }
    }

    fun process(it: SyndEntry): RSSDocument? {
        val existing = table.select(
            where = (table.link eq it.link).and(table.sourceCol eq "habr")
        ).firstOrNull()
        if (existing != null) {
            log.debug("Article '{}' exists", it.title)
            return null
        }
        log.info("Processing {}", it.title)
        val rss = RSSDocument(
            title = it.title,
            link = it.link,
            previewText = it.description.value,
            author = it.author,
            dt = LocalDateTime.ofInstant(it.publishedDate.toInstant(), ZoneOffset.UTC)
        )
        return rss
    }

    suspend fun parse(it: RSSDocument): ParsingResult {
        var soup: Document? = null
        client.prepareGet(it.link).execute { resp ->
            val stream = resp.bodyAsChannel().toInputStream()
            soup = Jsoup.parse(stream, "UTF-8", "")
        }
        val base = BaseArtcle(
            id = -1,
            title = it.title,
            source = "habr",
            link = it.link,
            ts = it.dt,
            previewText = it.previewText,
            previewPic = ""
        )
        soup?.let {
            val dtString = it.select("time")
                .first()?.attr("datetime")
            val dt = ZonedDateTime.parse(dtString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            val body = it.select("div#post-content-body")
            val pic = body.select("div img").first()?.attr("src") ?: ""
            val presenter = it.select("div.tm-article-presenter__footer ")
            val karma = presenter.select("div.tm-karma div.tm-karma__votes")
                .first()?.text()?.toInt()
                ?: return ParsingResult.PartialArticle(base, "could not fetch karma")
            val rating = presenter.select("div.tm-rating div.tm-rating__counter")
                .first()?.text()?.toDouble()
                ?: return ParsingResult.PartialArticle(base, "could not fetch rating")
            val name = presenter.select("div.tm-user-card__info a.tm-user-card__nickname")
                .first()?.text()
                ?: return ParsingResult.PartialArticle(base, "could not fetch author")
            val link = presenter.select("div.tm-user-card__info a.tm-user-card__nickname")
                .first()?.attr("href")
                ?: return ParsingResult.PartialArticle(base, "could not fetch link")
            val hubs = it.select(
                "div.tm-article-snippet__hubs span.tm-article-snippet__hubs-item span:nth-child(1)"
            ).map { hub -> hub.text() }
            val tags = it.select("ul > li > a.tm-tags-list__link")
                .map { tag -> tag.text() }
            val article = HabrArticle(
                base = base.copy(ts = dt.toLocalDateTime(), previewPic = pic),
                author = HabrAuthor(
                    name = name,
                    link = link,
                    karma = karma,
                    rating = rating.toInt()
                ),
                hubs = hubs,
                tags = tags
            )
            return ParsingResult.FullArticle(article)
        } ?: return ParsingResult.PartialArticle(base, "soup is null")
    }
}


sealed class ParsingResult {
    class FullArticle(val value: HabrArticle) : ParsingResult()
    class PartialArticle(val value: BaseArtcle, val reason: String) : ParsingResult()
}


class HabrFacade(
    val db: PrimaryDatabase,
    val service: HabrService
) {
    val log = logger()

    suspend fun importPage() {
        service.fetchPage()
            .flowOn(db.executor)
            .mapNotNull {
                db.transaction { service.process(it) }
            }.onEach { delay(Duration.ofMillis(2200)) }
            .map { service.parse(it) }
            .mapNotNull { result -> when(result) {
                is ParsingResult.PartialArticle -> {
                    log.error("Failed to parse article {} - {}", result.value.link, result.reason)
                    null
                }
                is ParsingResult.FullArticle -> result.value
            }}
            .collect { db.transaction { service.save(it) } }
    }

    suspend fun importLoop() {
        while (true) {
            try {
                importPage()
            } catch (e: IOException) {
                log.error("Error while importing articles", e)
            } catch (e: CancellationException) {
                return
            }
            delay(Duration.ofMinutes(1))
        }
    }
}


val HabrPlugin = createApplicationPlugin("HabrPlugin") {
    on(MonitoringEvent(ApplicationStarted)) { application -> application.launch {
        val facade = application.get<HabrFacade>()
        facade.importLoop()
    } }
}


fun Application.habrModule() {
    val mod = module {
        single { HabrService(get(named("feeds"))) }
        single { HabrFacade(get(), get()) }
    }
    loadKoinModules(mod)
//    install(HabrPlugin)
}
