package org.grails.datastore.gorm.neo4j

import grails.gorm.tests.GormDatastoreSpec
import grails.gorm.dirty.checking.DirtyCheck
import grails.neo4j.Neo4jEntity
import grails.persistence.Entity

class DynamicAssociationNameSpec extends GormDatastoreSpec {

    @Override
    List getDomainClasses() {
        [Person, House]
    }

    def "test cypher queries"() {
        setup:
        House house = new House(address: 'Street 1').save()
        Person person = new Person(username: 'user1')
        person.firstname = 'person1'
        person.liveIn = house
        person.save()
        session.flush()
        session.clear()

        when: 'ask for a simple property'
        def result = Person.executeCypher("MATCH (p:Person) WHERE p.firstname='person1' RETURN p")

        then:
        result.iterator().size() == 1

        when: 'ask for a simple property'
        result = Person.executeCypher("MATCH (p:Person) WHERE p.firstname={firstname} RETURN p", [firstname:'person1'])

        then:
        result.iterator().size() == 1

        when: 'ask for a relationship. The attribute is in camelCase, but it is in SNAKE_CASE in the data base'
        result = Person.executeCypher("MATCH (p:Person)-[:LIVE_IN]->(h:House) RETURN h")

        then:
        result.iterator().size() == 1
    }

    def "test load dynamic entities"() {
        setup:
        House house = new House(address: 'Street 1').save()
        Person person = new Person(username: 'user1')
        person.firstname = 'person1'
        person.liveIn = house
        person.save()
        session.flush()
        session.clear()

        when: 'ask for the person'
        def result = Person.findByUsername('user1')

        then:
        result.liveIn.address == 'Street 1'
    }
}

@Entity
@DirtyCheck
class Person implements Neo4jEntity<Person> {

    Long id
    Long version
    String username

    static mapping = {
        dynamicAssociations true
    }
}

@Entity
@DirtyCheck
class House implements Neo4jEntity<House> {

    Long id
    Long version
    String address
}

