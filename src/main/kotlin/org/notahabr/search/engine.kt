package org.notahabr.search

import cz.jirutka.rsql.parser.ast.*
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.greater
import org.jetbrains.exposed.sql.SqlExpressionBuilder.greaterEq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.inList
import org.jetbrains.exposed.sql.SqlExpressionBuilder.less
import org.jetbrains.exposed.sql.SqlExpressionBuilder.lessEq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.neq
import org.notahabr.core.tables.ArticlesTable
import org.notahabr.feed.HabrPosts
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter


object Operators {
    val has = ComparisonOperator("=has=", false)
    val hasNot = ComparisonOperator("=hasnot=", false)
}

class FeedQueryVisitor : RSQLVisitor<Op<Boolean>, Unit> {
    val builder = SearchQueryBuilder()

    override fun visit(node: AndNode, param: Unit?): Op<Boolean> {
        return builder.createQuery(node)
    }

    override fun visit(node: ComparisonNode, param: Unit?): Op<Boolean> {
        return builder.createQuery(node)
    }

    override fun visit(node: OrNode, param: Unit?): Op<Boolean> {
        return builder.createQuery(node)
    }
}

class SearchQueryBuilder {
    fun createQuery(node: Node): Op<Boolean>? = when(node) {
        is LogicalNode -> createQuery(node)
        is ComparisonNode -> createQuery(node)
        else -> null
    }

    fun createQuery(node: LogicalNode): Op<Boolean> {
        val exprs = node.children
            .map { createQuery(it) }
            .filterNotNull()
        var result = exprs[0]
        if (node.operator == LogicalOperator.AND) {
            exprs.forEach {
                result = result.and(it)
            }
        } else if (node.operator == LogicalOperator.OR) {
            exprs.forEach {
                result = result.or(it)
            }
        }
        return result
    }

    fun createQuery(node: ComparisonNode): Op<Boolean> {
        return SearchQuery(node.selector, node.operator, node.arguments)
    }
}

class SearchQuery(
    val selector: String,
    val operator: ComparisonOperator,
    val args: List<String>
) : Op<Boolean>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) {
        val expr = when (selector) {
            "source" -> ArticlesTable.sourceCol.applyStringOp()
            "title" -> ArticlesTable.title.applyStringOp()
            "ts" -> ArticlesTable.ts.applyDateOp()
            "habr.hubs" -> HabrPosts.hubs.applyArrayOp()
            "habr.tags" -> HabrPosts.tags.applyArrayOp()
            "habr.author.name" -> HabrPosts.authorName.applyStringOp()
            "habr.author.karma" -> HabrPosts.authorKarma.applyIntOp()
            "habr.author.rating" -> HabrPosts.authorRating.applyIntOp()
            else -> nullOp()
        }
        queryBuilder.append(expr)
    }

    fun Column<String>.applyStringOp(): Op<Boolean> {
        return when (operator) {
            RSQLOperators.EQUAL -> eq(args[0])
            RSQLOperators.IN -> inList(args)
            RSQLOperators.NOT_EQUAL -> not(eq(args[0]))
            RSQLOperators.NOT_IN -> not(inList(args))
            else -> nullOp()
        }
    }

    fun Column<LocalDateTime>.applyDateOp(): Op<Boolean> {
        val arg = args[0].toLDT()
        return when(operator) {
            RSQLOperators.EQUAL -> eq(arg)
            RSQLOperators.NOT_EQUAL -> eq(arg)
            RSQLOperators.GREATER_THAN -> greater(arg)
            RSQLOperators.GREATER_THAN_OR_EQUAL -> greaterEq(arg)
            RSQLOperators.LESS_THAN -> less(arg)
            RSQLOperators.LESS_THAN_OR_EQUAL -> lessEq(arg)
            else -> nullOp()
        }
    }

    fun String.toLDT() = ZonedDateTime
        .parse(this, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .toLocalDateTime()

    fun Column<Array<String>>.applyArrayOp(): Op<Boolean> {
        return when(operator) {
            Operators.has -> has(args[0])
            Operators.hasNot -> not(has(args[0]))
            else -> nullOp()
        }
    }

    fun Column<Array<String>>.has(item: String) = HasOp(this, item)

    fun Column<Int>.applyIntOp(): Op<Boolean> {
        val arg = args[0].toInt()
        return when (operator) {
            RSQLOperators.EQUAL -> eq(arg)
            RSQLOperators.NOT_EQUAL -> neq(arg)
            RSQLOperators.GREATER_THAN -> greater(arg)
            RSQLOperators.GREATER_THAN_OR_EQUAL -> greaterEq(arg)
            RSQLOperators.LESS_THAN -> less(arg)
            RSQLOperators.LESS_THAN_OR_EQUAL -> lessEq(arg)
            else -> nullOp()
        }
    }
}

class HasOp(val col: Column<Array<String>>, val item: String) : Op<Boolean>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) {
        queryBuilder.append("'$item'")
        queryBuilder.append(" = ")
        queryBuilder.append("ANY")
        queryBuilder.append("(${col.nameInDatabaseCase()})")
    }
}