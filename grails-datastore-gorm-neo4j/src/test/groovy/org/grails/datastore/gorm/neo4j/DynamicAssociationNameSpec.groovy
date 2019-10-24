package org.grails.datastore.gorm.neo4j

import grails.gorm.tests.GormDatastoreSpec
import grails.gorm.dirty.checking.DirtyCheck
import grails.neo4j.Neo4jEntity
import grails.persistence.Entity

class DynamicAssociationNameSpec extends GormDatastoreSpec {

    @Override
    List getDomainClasses() {
        [Subject, Degree, Employee]
    }

    def "test cypher queries convertDynamicAssociationNames=true"() {
        setup:
        Subject subject1 = new Subject(name: 'Differential Equations').save()

        Subject subject2 = new Subject(name: 'Calculus').save()

        Employee employee = new Employee(name: 'Stuart').save()

        String degreeName = "Bachelor of Science in Mathematics"
        Degree degree = new Degree(name: degreeName)
        degree.internalCode = 'CM18'
        degree.optionalSubjects = [subject1]
        degree.mandatorySubjects = [subject2]
        degree.inChargeOf = employee
        degree.save()

        session.flush()
        session.clear()

        when: 'ask for a simple property. The property is not converted'
        def result = Degree.executeCypher("MATCH (d:Degree) WHERE d.internalCode='CM18' RETURN d")

        then:
        result.iterator().size() == 1

        when:
        result = Degree.executeCypher("MATCH (d:Degree) WHERE d.INTERNAL_CODE='CM18' RETURN d")

        then:
        result.iterator().size() == 0

        when: 'ask for a relationship OneToMany, the relationship name is in camelCase, but it is in SNAKE_CASE in the data base'
        result = Degree.executeCypher("MATCH (d:Degree)-[:OPTIONAL_SUBJECTS]->(s:Subject) RETURN s")

        then:
        result.iterator().size() == 1

        when:
        result = Degree.executeCypher("MATCH (d:Degree)-[:optionalSubjects]->(s:Subject) RETURN s")

        then:
        result.iterator().size() == 0

        when: 'ask for a relationship OneToOne, the relationship name is in camelCase, but it is in SNAKE_CASE in the data base'
        result = Degree.executeCypher("MATCH (d:Degree)-[:IN_CHARGE_OF]->(e:Employee) RETURN e")

        then:
        result.iterator().size() == 1

        when:
        result = Degree.executeCypher("MATCH (d:Degree)-[:inChargeOf]->(e:Employee) RETURN e")

        then:
        result.iterator().size() == 0
    }

    def "test cypher queries convertDynamicAssociationNames is not defined"() {
        setup:
        Employee employee = new Employee(name: 'Adam').save()

        Subject subject1 = new Subject(name: 'Differential Equations')
        subject1.inChargeOf = employee
        subject1.save()

        session.flush()
        session.clear()

        when:
        def result = Degree.executeCypher("MATCH (s:Subject)-[:inChargeOf]->(e:Employee) RETURN e")

        then:
        result.iterator().size() == 1

        when:
        result = Degree.executeCypher("MATCH (s:Subject)-[:IN_CHARGE_OF]->(e:Employee) RETURN e")

        then:
        result.iterator().size() == 0
    }

    def "test load dynamic entities"() {
        setup:
        Subject subject1 = new Subject(name: 'Differential Equations').save()
        Subject subject2 = new Subject(name: 'Calculus').save()

        Employee employee = new Employee(name: 'Stuart').save()

        String degreeName = "Bachelor of Science in Mathematics"
        Degree degree = new Degree(name: degreeName)
        degree.internalCode = 'CM18'
        degree.optionalSubjects = [subject1]
        degree.mandatorySubjects = [subject2]
        degree.inChargeOf = employee
        degree.save()

        session.flush()
        session.clear()

        when: 'ask for the person'
        def result = Degree.findByInternalCode('CM18')

        then:
        result.optionalSubjects[0].name == 'Differential Equations'
        result.mandatorySubjects[0].name == 'Calculus'
        result.inChargeOf.name == 'Stuart'
    }
}

@Entity
@DirtyCheck
class Degree implements Neo4jEntity<Degree> {

    Long id
    Long version
    String name

    static mapping = {
        dynamicAssociations true
        convertDynamicAssociationNames true
    }
}

@Entity
@DirtyCheck
class Subject implements Neo4jEntity<Subject> {

    Long id
    Long version
    String name

    static mapping = {
        dynamicAssociations true
    }
}

@Entity
@DirtyCheck
class Employee implements Neo4jEntity<Employee> {

    Long id
    Long version
    String name
}

