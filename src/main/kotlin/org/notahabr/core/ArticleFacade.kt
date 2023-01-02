package org.notahabr.core

import cz.jirutka.rsql.parser.RSQLParser
import cz.jirutka.rsql.parser.ast.RSQLOperators
import org.jetbrains.exposed.sql.Op
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.select
import org.notahabr.core.tables.ArticlesTable
import org.notahabr.feed.BaseArtcle
import org.notahabr.feed.HabrPosts
import org.notahabr.search.FeedQueryVisitor
import org.notahabr.search.Operators
import org.notahabr.subsystem.PrimaryDatabase

class ArticleFacade(
    val db: PrimaryDatabase
) {
    suspend fun searchBy(query: String): List<BaseArtcle> {
        val ops = RSQLOperators.defaultOperators()
        ops.addAll(listOf(Operators.has, Operators.hasNot))
        val node = RSQLParser(ops).parse(query)
        val op = node.accept(FeedQueryVisitor())
        return db.transaction {
            searchBy(op)
        }
    }

    fun searchBy(where: Op<Boolean>): List<BaseArtcle> {
        val rows = (ArticlesTable innerJoin HabrPosts)
            .select(where)
            .limit(10)
            .map { BaseArtcle(
                id = it[ArticlesTable.id].value,
                title = it[ArticlesTable.title],
                source = it[ArticlesTable.sourceCol],
                link = it[ArticlesTable.link],
                ts = it[ArticlesTable.ts],
                previewText = it[ArticlesTable.previewText],
                previewPic = it[ArticlesTable.previewPic]
            ) }
        return rows
    }
}