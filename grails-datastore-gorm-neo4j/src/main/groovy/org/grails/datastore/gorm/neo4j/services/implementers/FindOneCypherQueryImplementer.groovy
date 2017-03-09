package org.grails.datastore.gorm.neo4j.services.implementers

import grails.gorm.services.Cypher
import groovy.transform.CompileStatic
import org.codehaus.groovy.ast.VariableScope
import org.codehaus.groovy.control.SourceUnit
import org.grails.datastore.gorm.neo4j.services.transform.CypherQueryStringTransformer
import org.grails.datastore.gorm.services.implementers.FindOneStringQueryImplementer
import org.grails.datastore.gorm.services.transform.QueryStringTransformer

import java.lang.annotation.Annotation

/**
 * A cypher query implementer
 *
 * @author Graeme Rocher
 * @since 6.1
 */
@CompileStatic
class FindOneCypherQueryImplementer extends FindOneStringQueryImplementer {


    @Override
    protected Class<? extends Annotation> getAnnotationType() {
        Cypher
    }

    @Override
    protected QueryStringTransformer createQueryStringTransformer(SourceUnit sourceUnit, VariableScope scope) {
        return new CypherQueryStringTransformer(sourceUnit, scope)
    }
}
