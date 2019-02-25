/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.test.insertordering;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import org.hibernate.Session;
import org.hibernate.annotations.BatchSize;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.jdbc.batch.internal.BatchBuilderImpl;
import org.hibernate.engine.jdbc.batch.internal.BatchBuilderInitiator;
import org.hibernate.engine.jdbc.batch.internal.BatchingBatch;
import org.hibernate.engine.jdbc.batch.spi.Batch;
import org.hibernate.engine.jdbc.batch.spi.BatchKey;
import org.hibernate.engine.jdbc.spi.JdbcCoordinator;

import org.hibernate.testing.FailureExpected;
import org.hibernate.testing.TestForIssue;
import org.hibernate.testing.junit4.BaseCoreFunctionalTestCase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Steve Ebersole
 */
@TestForIssue( jiraKey = "HHH-9864" )
public class InsertOrderingWithInheritance extends BaseCoreFunctionalTestCase {

	@Test
	public void testBatchOrdering() {
		Session session = openSession();
		session.getTransaction().begin();

		// First object with dependent object (address)
		final Person person = new Person();
		person.addAddress(new Address());
		session.persist(person);

		// Derived Object with dependent object (address)
		final SpecialPerson specialPerson = new SpecialPerson();
		specialPerson.addAddress(new Address());
		session.persist( specialPerson );

		session.getTransaction().commit();
		session.close();

		cleanupData();
	}

	@Test
	public void testBatchingAmongstSubClasses() {
		Session session = openSession();
		session.getTransaction().begin();
		int iterations = 12;
		for ( int i = 0; i < iterations; i++ ) {
			final Person person = new Person();
			person.addAddress( new Address() );
			session.persist( person );

			final SpecialPerson specialPerson = new SpecialPerson();
			specialPerson.addAddress(new Address());
			session.persist( specialPerson );
		}
		session.getTransaction().commit();
		session.close();

		cleanupData();
	}

	private void cleanupData() {
		Session session = openSession();
		session.getTransaction().begin();
		session.createQuery( "delete Address" ).executeUpdate();
		session.createQuery( "delete Person" ).executeUpdate();
		session.getTransaction().commit();
		session.close();
	}

	@Override
	protected Class[] getAnnotatedClasses() {
		return new Class[] { Address.class, Person.class, SpecialPerson.class };
	}

	@Override
	protected void configure(Configuration cfg) {
		super.configure( cfg );
		cfg.setProperty( Environment.ORDER_INSERTS, "true" );
		cfg.setProperty( Environment.STATEMENT_BATCH_SIZE, "10" );
	}

	@Entity(name = "Address")
	@Table(name = "ADDRESS")
	@Access(AccessType.FIELD)
	public static class Address {
		@Id
		@Column(name = "ID", nullable = false)
		@SequenceGenerator(name = "ID", sequenceName = "ADDRESS_SEQ")
		@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ID")
		private Long id;
	}

	@Entity( name = "Person" )
	@Access(AccessType.FIELD)
	@Table(name = "PERSON")
	@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
	@DiscriminatorColumn(name = "CLASSINDICATOR", discriminatorType = DiscriminatorType.INTEGER)
	@DiscriminatorValue("1")
	public static class Person {
		@Id
		@Column(name = "ID", nullable = false)
		@SequenceGenerator(name = "ID", sequenceName = "PERSON_SEQ")
		@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ID")
		private Long id;

		@OneToMany(orphanRemoval = true, cascade = {CascadeType.PERSIST, CascadeType.REMOVE})
		@JoinColumn(name = "PERSONID", referencedColumnName = "ID", nullable = false, updatable = false)
		@BatchSize(size = 100)
		private Set<Address> addresses = new HashSet<Address>();

		public void addAddress(Address address) {
			this.addresses.add(address);
		}

	}

	@Entity
	@Access(AccessType.FIELD)
	@DiscriminatorValue("2")
	public static class SpecialPerson extends Person {
		@Column(name = "special")
		private String special;
	}

}
