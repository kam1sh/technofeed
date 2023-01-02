package org.notahabr.core.tables

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.serializer
import org.jetbrains.exposed.dao.id.LongIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.Function
import org.jetbrains.exposed.sql.javatime.datetime
import org.postgresql.util.PGobject

object ArticlesTable : LongIdTable("articles") {
    val title = varchar("title", 256)
    val sourceCol = varchar("source", 64)
    val link = text("link")
    val ts = datetime("ts")
    val previewText = text("preview_text")
    val previewPic = text("preview_pic")
}

inline fun <reified T : Any> Table.jsonb(
    name: String,
    kSerializer: KSerializer<T> = serializer(),
    json: Json
): Column<T> =
    this.json(
        name = name,
        stringify = { json.encodeToString(kSerializer, it) },
        parse = { json.decodeFromString(kSerializer, it) },
    )

fun <T : Any> Table.json(name: String, stringify: (T) -> String, parse: (String) -> T): Column<T> =
    registerColumn(name, JsonColumnType(stringify, parse))

class JsonColumnType<T : Any>(private val stringify: (T) -> String, private val parse: (String) -> T) : ColumnType() {
    override fun sqlType(): String = "json"
    override fun valueFromDB(value: Any) = (value as PGobject).value!!.let(parse)

    override fun valueToDB(value: Any?): Any {
        val obj = PGobject()
        obj.type = "jsonb"
        obj.value = stringify(value as T)
        return obj
    }
}

inline infix fun <reified T> Column<out Collection<T>>.contains(elem: T) =
    contains(elem, Json, serializer())

inline fun <reified T> Column<out Collection<T>>.contains(elem: T, json: Json, serializer: KSerializer<T>) = Op.build {
    JsonContains(this@contains, elem) { json.encodeToString(serializer, it) } eq true
}

fun <T> Column<out Collection<T>>.contains(elem: T, serialize: (T) -> String) = Op.build {
    JsonContains(this@contains, elem, serialize) eq true
}

// there is probably a better way of doing this
class JsonContains<out T>(
    private val column: Column<out Collection<T>>,
    private val value: T,
    private val serialize: (T) -> String,
) : Function<Boolean>(BooleanColumnType()) {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) = queryBuilder {
        append("JSON_CONTAINS(")
        append(column)
        append(',')
        append("'")
        append(serialize(value))
        append("'")
        append(")")
    }
}




