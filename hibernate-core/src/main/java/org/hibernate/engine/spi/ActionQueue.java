/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2010, Red Hat Inc. or third-party contributors as
 * indicated by the @author tags or express copyright attribution
 * statements applied by the authors.  All third-party contributions are
 * distributed under license by Red Hat Inc.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.hibernate.engine.spi;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.PropertyValueException;
import org.hibernate.action.internal.AbstractEntityInsertAction;
import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.action.internal.CollectionRecreateAction;
import org.hibernate.action.internal.CollectionRemoveAction;
import org.hibernate.action.internal.CollectionUpdateAction;
import org.hibernate.action.internal.EntityDeleteAction;
import org.hibernate.action.internal.EntityIdentityInsertAction;
import org.hibernate.action.internal.EntityInsertAction;
import org.hibernate.action.internal.EntityUpdateAction;
import org.hibernate.action.internal.OrphanRemovalAction;
import org.hibernate.action.internal.QueuedOperationCollectionAction;
import org.hibernate.action.internal.UnresolvedEntityInsertActions;
import org.hibernate.action.spi.AfterTransactionCompletionProcess;
import org.hibernate.action.spi.BeforeTransactionCompletionProcess;
import org.hibernate.action.spi.Executable;
import org.hibernate.cache.CacheException;
import org.hibernate.engine.internal.NonNullableTransientDependencies;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.tuple.entity.EntityMetamodel;
import org.hibernate.type.CollectionType;
import org.hibernate.type.CompositeType;
import org.hibernate.type.EntityType;
import org.hibernate.type.ForeignKeyDirection;
import org.hibernate.type.OneToOneType;
import org.hibernate.type.Type;

/**
 * Responsible for maintaining the queue of actions related to events.
 *
 * The ActionQueue holds the DML operations queued as part of a session's transactional-write-behind semantics. The
 * DML operations are queued here until a flush forces them to be executed against the database.
 * 
 * @author Steve Ebersole
 * @author Gail Badner
 * @author Anton Marsden
 */
public class ActionQueue {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( ActionQueue.class );

	private SessionImplementor session;

	private UnresolvedEntityInsertActions unresolvedInsertions;

	// Object insertions, updates, and deletions have list semantics because
	// they must happen in the right order so as to respect referential
	// integrity
	private final ExecutableList<AbstractEntityInsertAction> insertions;
	private final ExecutableList<EntityDeleteAction> deletions;
	private final ExecutableList<EntityUpdateAction> updates;

	// Actually the semantics of the next three are really "Bag"
	// Note that, unlike objects, collection insertions, updates,
	// deletions are not really remembered between flushes. We
	// just re-use the same Lists for convenience.
	private final ExecutableList<CollectionRecreateAction> collectionCreations;
	private final ExecutableList<CollectionUpdateAction> collectionUpdates;
	private final ExecutableList<QueuedOperationCollectionAction> collectionQueuedOps;
	private final ExecutableList<CollectionRemoveAction> collectionRemovals;
	
	// TODO: The removeOrphan concept is a temporary "hack" for HHH-6484.  This should be removed once action/task
	// ordering is improved.
	private final ExecutableList<OrphanRemovalAction> orphanRemovals;

	// an immutable array holding all 7 ExecutionLists in execution order
	private final List<ExecutableList<?>> executableLists;

	private transient boolean isTransactionCoordinatorShared;
	private AfterTransactionCompletionProcessQueue afterTransactionProcesses;
	private BeforeTransactionCompletionProcessQueue beforeTransactionProcesses;

	/**
	 * Constructs an action queue bound to the given session.
	 * 
	 * @param session The session "owning" this queue.
	 */
	public ActionQueue(SessionImplementor session) {
		this.session = session;

		unresolvedInsertions = new UnresolvedEntityInsertActions();

		insertions = new ExecutableList<AbstractEntityInsertAction>( new InsertActionSorter() );
		deletions = new ExecutableList<EntityDeleteAction>();
		updates = new ExecutableList<EntityUpdateAction>();

		collectionCreations = new ExecutableList<CollectionRecreateAction>();
		collectionRemovals = new ExecutableList<CollectionRemoveAction>();
		collectionUpdates = new ExecutableList<CollectionUpdateAction>();
		collectionQueuedOps = new ExecutableList<QueuedOperationCollectionAction>();
		
		orphanRemovals = new ExecutableList<OrphanRemovalAction>();

		// Important: these lists are in execution order
		List<ExecutableList<?>> tmp = Arrays.<ExecutableList<?>>asList(
				orphanRemovals,
				insertions,
				updates,
				// do before actions are handled in the other collection queues
				collectionQueuedOps,
				collectionRemovals,
				collectionUpdates,
				collectionCreations,
				deletions
		);

		executableLists = Collections.unmodifiableList( tmp );

		isTransactionCoordinatorShared = false;
		afterTransactionProcesses = new AfterTransactionCompletionProcessQueue( session );
		beforeTransactionProcesses = new BeforeTransactionCompletionProcessQueue( session );

	}

	public void clear() {
		for ( ExecutableList<?> l : executableLists ) {
			l.clear();
		}
		unresolvedInsertions.clear();
	}

	/**
	 * Adds an entity insert action
	 *
	 * @param action The action representing the entity insertion
	 */
	public void addAction(EntityInsertAction action) {
		LOG.tracev( "Adding an EntityInsertAction for [{0}] object", action.getEntityName() );
		addInsertAction( action );
	}

	private void addInsertAction(AbstractEntityInsertAction insert) {
		if ( insert.isEarlyInsert() ) {
			// For early inserts, must execute inserts before finding non-nullable transient entities.
			// TODO: find out why this is necessary
			LOG.tracev( "Executing inserts before finding non-nullable transient entities for early insert: [{0}]", insert );
			executeInserts();
		}
		NonNullableTransientDependencies nonNullableTransientDependencies = insert.findNonNullableTransientEntities();
		if ( nonNullableTransientDependencies == null ) {
			LOG.tracev( "Adding insert with no non-nullable, transient entities: [{0}]", insert );
			addResolvedEntityInsertAction( insert );
		}
		else {
			if ( LOG.isTraceEnabled() ) {
				LOG.tracev( "Adding insert with non-nullable, transient entities; insert=[{0}], dependencies=[{1}]", insert,
							nonNullableTransientDependencies.toLoggableString( insert.getSession() ) );
			}
			unresolvedInsertions.addUnresolvedEntityInsertAction( insert, nonNullableTransientDependencies );
		}
	}

	private void addResolvedEntityInsertAction(AbstractEntityInsertAction insert) {
		if ( insert.isEarlyInsert() ) {
			LOG.trace( "Executing insertions before resolved early-insert" );
			executeInserts();
			LOG.debug( "Executing identity-insert immediately" );
			execute( insert );
		}
		else {
			LOG.trace( "Adding resolved non-early insert action." );
			insertions.add( insert );
		}
		insert.makeEntityManaged();
		for ( AbstractEntityInsertAction resolvedAction : unresolvedInsertions.resolveDependentActions( insert.getInstance(), session ) ) {
			addResolvedEntityInsertAction( resolvedAction );
		}
	}

	/**
	 * Adds an entity (IDENTITY) insert action
	 *
	 * @param action The action representing the entity insertion
	 */
	public void addAction(EntityIdentityInsertAction action) {
		LOG.tracev( "Adding an EntityIdentityInsertAction for [{0}] object", action.getEntityName() );
		addInsertAction( action );
	}

	/**
	 * Adds an entity delete action
	 *
	 * @param action The action representing the entity deletion
	 */
	public void addAction(EntityDeleteAction action) {
		deletions.add( action );
	}

	/**
	 * Adds an orphan removal action
	 *
	 * @param action The action representing the orphan removal
	 */
	public void addAction(OrphanRemovalAction action) {
		orphanRemovals.add( action );
	}

	/**
	 * Adds an entity update action
	 *
	 * @param action The action representing the entity update
	 */
	public void addAction(EntityUpdateAction action) {
		updates.add( action );
	}

	/**
	 * Adds a collection (re)create action
	 *
	 * @param action The action representing the (re)creation of a collection
	 */
	public void addAction(CollectionRecreateAction action) {
		collectionCreations.add( action );
	}

	/**
	 * Adds a collection remove action
	 *
	 * @param action The action representing the removal of a collection
	 */
	public void addAction(CollectionRemoveAction action) {
		collectionRemovals.add( action );
	}

	/**
	 * Adds a collection update action
	 *
	 * @param action The action representing the update of a collection
	 */
	public void addAction(CollectionUpdateAction action) {
		collectionUpdates.add( action );
	}

	/**
	 * Adds an action relating to a collection queued operation (extra lazy).
	 *
	 * @param action The action representing the queued operation
	 */
	public void addAction(QueuedOperationCollectionAction action) {
		collectionQueuedOps.add( action );
	}

	/**
	 * Adds an action defining a cleanup relating to a bulk operation (HQL/JPQL or Criteria based update/delete)
	 *
	 * @param action The action representing the queued operation
	 */
	public void addAction(BulkOperationCleanupAction action) {
		registerCleanupActions( action );
	}

	private void registerCleanupActions(Executable executable) {
		beforeTransactionProcesses.register( executable.getBeforeTransactionCompletionProcess() );
		if ( session.getFactory().getSettings().isQueryCacheEnabled() ) {
			invalidateSpaces( executable.getPropertySpaces() );
		}
		afterTransactionProcesses.register( executable.getAfterTransactionCompletionProcess() );
	}

	/**
	 * Are there unresolved entity insert actions that depend on non-nullable associations with a transient entity?
	 * 
	 * @return true, if there are unresolved entity insert actions that depend on non-nullable associations with a
	 * transient entity; false, otherwise
	 */
	public boolean hasUnresolvedEntityInsertActions() {
		return !unresolvedInsertions.isEmpty();
	}

	/**
	 * Throws {@link org.hibernate.PropertyValueException} if there are any unresolved entity insert actions that depend
	 * on non-nullable associations with a transient entity. This method should be called on completion of an operation
	 * (after all cascades are completed) that saves an entity.
	 * 
	 * @throws org.hibernate.PropertyValueException if there are any unresolved entity insert actions;
	 * {@link org.hibernate.PropertyValueException#getEntityName()} and
	 * {@link org.hibernate.PropertyValueException#getPropertyName()} will return the entity name and property value for
	 * the first unresolved entity insert action.
	 */
	public void checkNoUnresolvedActionsAfterOperation() throws PropertyValueException {
		unresolvedInsertions.checkNoUnresolvedActionsAfterOperation();
	}

	public void registerProcess(AfterTransactionCompletionProcess process) {
		afterTransactionProcesses.register( process );
	}

	public void registerProcess(BeforeTransactionCompletionProcess process) {
		beforeTransactionProcesses.register( process );
	}

	/**
	 * Perform all currently queued entity-insertion actions.
	 * 
	 * @throws HibernateException error executing queued insertion actions.
	 */
	public void executeInserts() throws HibernateException {
		executeActions( insertions );
	}

	/**
	 * Perform all currently queued actions.
	 * 
	 * @throws HibernateException error executing queued actions.
	 */
	public void executeActions() throws HibernateException {
		if ( !unresolvedInsertions.isEmpty() ) {
			throw new IllegalStateException( "About to execute actions, but there are unresolved entity insert actions." );
		}

		for ( ExecutableList<?> l : executableLists ) {
			executeActions( l );
		}
	}

	/**
	 * Prepares the internal action queues for execution.
	 * 
	 * @throws HibernateException error preparing actions.
	 */
	public void prepareActions() throws HibernateException {
		prepareActions( collectionRemovals );
		prepareActions( collectionUpdates );
		prepareActions( collectionCreations );
		prepareActions( collectionQueuedOps );
	}

	private void prepareActions(ExecutableList<?> queue) throws HibernateException {
		for ( Executable executable : queue ) {
			executable.beforeExecutions();
		}
	}

	/**
	 * Performs cleanup of any held cache softlocks.
	 * 
	 * @param success Was the transaction successful.
	 */
	public void afterTransactionCompletion(boolean success) {
		if ( !isTransactionCoordinatorShared ) {
			// Execute completion actions only in transaction owner (aka parent session).
			afterTransactionProcesses.afterTransactionCompletion( success );
		}
	}

	/**
	 * Execute any registered {@link org.hibernate.action.spi.BeforeTransactionCompletionProcess}
	 */
	public void beforeTransactionCompletion() {
		if ( !isTransactionCoordinatorShared ) {
			// Execute completion actions only in transaction owner (aka parent session).
			beforeTransactionProcesses.beforeTransactionCompletion();
		}
	}

	/**
	 * Check whether any insertion or deletion actions are currently queued.
	 *
	 * @return {@code true} if insertions or deletions are currently queued; {@code false} otherwise.
	 */
	public boolean areInsertionsOrDeletionsQueued() {
		return !insertions.isEmpty() || !unresolvedInsertions.isEmpty() || !deletions.isEmpty() || !orphanRemovals.isEmpty();
	}

	/**
	 * Check whether the given tables/query-spaces are to be executed against given the currently queued actions.
	 * 
	 * @param tables The table/query-spaces to check.
	 *
	 * @return {@code true} if we contain pending actions against any of the given tables; {@code false} otherwise.
	 */
	public boolean areTablesToBeUpdated(@SuppressWarnings("rawtypes") Set tables) {
		if ( tables.isEmpty() ) {
			return false;
		}
		for ( ExecutableList<?> l : executableLists ) {
			if ( areTablesToBeUpdated( l, tables ) ) {
				return true;
			}
		}
		return areTablesToBeUpdated( unresolvedInsertions, tables );
	}

	private static boolean areTablesToBeUpdated(ExecutableList<?> actions, @SuppressWarnings("rawtypes") Set tableSpaces) {
		if ( actions.isEmpty() ) {
			return false;
		}

		for ( Serializable actionSpace : actions.getQuerySpaces() ) {
			if ( tableSpaces.contains( actionSpace ) ) {
				LOG.debugf( "Changes must be flushed to space: %s", actionSpace );
				return true;
			}
		}

		return false;
	}

	private static boolean areTablesToBeUpdated(UnresolvedEntityInsertActions actions, @SuppressWarnings("rawtypes") Set tableSpaces) {
		for ( Executable action : actions.getDependentEntityInsertActions() ) {
			final Serializable[] spaces = action.getPropertySpaces();
			for ( Serializable space : spaces ) {
				if ( tableSpaces.contains( space ) ) {
					LOG.debugf( "Changes must be flushed to space: %s", space );
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Perform {@link org.hibernate.action.spi.Executable#execute()} on each element of the list
	 * 
	 * @param list The list of Executable elements to be performed
	 *
	 * @throws HibernateException
	 */
	private <E extends Executable & Comparable<?> & Serializable> void executeActions(ExecutableList<E> list) throws HibernateException {
		// todo : consider ways to improve the double iteration of Executables here:
		//		1) we explicitly iterate list here to perform Executable#execute()
		//		2) ExecutableList#getQuerySpaces also iterates the Executables to collect query spaces.
		try {
			for ( E e : list ) {
				try {
					e.execute();
				}
				finally {
					beforeTransactionProcesses.register( e.getBeforeTransactionCompletionProcess() );
					afterTransactionProcesses.register( e.getAfterTransactionCompletionProcess() );
				}
			}
		}
		finally {
			if ( session.getFactory().getSettings().isQueryCacheEnabled() ) {
				// Strictly speaking, only a subset of the list may have been processed if a RuntimeException occurs.
				// We still invalidate all spaces. I don't see this as a big deal - after all, RuntimeExceptions are
				// unexpected.
				Set<Serializable> propertySpaces = list.getQuerySpaces();
				invalidateSpaces( propertySpaces.toArray( new Serializable[propertySpaces.size()] ) );
			}
		}

		list.clear();
		session.getTransactionCoordinator().getJdbcCoordinator().executeBatch();
	}

	/**
	 * @param executable The action to execute
	 */
	public <E extends Executable & Comparable<?>> void execute(E executable) {
		try {
			executable.execute();
		}
		finally {
			registerCleanupActions( executable );
		}
	}

	/**
	 * This method is now called once per execution of an ExecutableList or once for execution of an Execution.
	 * 
	 * @param spaces The spaces to invalidate
	 */
	private void invalidateSpaces(Serializable... spaces) {
		if ( spaces != null && spaces.length > 0 ) {
			for ( Serializable s : spaces ) {
				afterTransactionProcesses.addSpaceToInvalidate( (String) s );
			}
			// Performance win: If we are processing an ExecutableList, this will only be called once
			session.getFactory().getUpdateTimestampsCache().preInvalidate( spaces, session );
		}
	}

	/**
	 * Returns a string representation of the object.
	 * 
	 * @return a string representation of the object.
	 */
	@Override
	public String toString() {
		return "ActionQueue[insertions=" + insertions
				+ " updates=" + updates
				+ " deletions=" + deletions
				+ " orphanRemovals=" + orphanRemovals
				+ " collectionCreations=" + collectionCreations
				+ " collectionRemovals=" + collectionRemovals
				+ " collectionUpdates=" + collectionUpdates
				+ " collectionQueuedOps=" + collectionQueuedOps
				+ " unresolvedInsertDependencies=" + unresolvedInsertions
				+ "]";
	}

	public int numberOfCollectionRemovals() {
		return collectionRemovals.size();
	}

	public int numberOfCollectionUpdates() {
		return collectionUpdates.size();
	}

	public int numberOfCollectionCreations() {
		return collectionCreations.size();
	}

	public int numberOfDeletions() {
		return deletions.size() + orphanRemovals.size();
	}

	public int numberOfUpdates() {
		return updates.size();
	}

	public int numberOfInsertions() {
		return insertions.size();
	}

	public TransactionCompletionProcesses getTransactionCompletionProcesses() {
		return new TransactionCompletionProcesses( beforeTransactionProcesses, afterTransactionProcesses );
	}

	/**
	 * Bind transaction completion processes to make them shared between primary and secondary session.
	 * Transaction completion processes are always executed by transaction owner (primary session),
	 * but can be registered using secondary session too.
	 *
	 * @param processes Transaction completion processes.
	 * @param isTransactionCoordinatorShared Flag indicating shared transaction context.
	 */
	public void setTransactionCompletionProcesses(TransactionCompletionProcesses processes, boolean isTransactionCoordinatorShared) {
		this.isTransactionCoordinatorShared = isTransactionCoordinatorShared;
		this.beforeTransactionProcesses = processes.beforeTransactionCompletionProcesses;
		this.afterTransactionProcesses = processes.afterTransactionCompletionProcesses;
	}

	public void sortCollectionActions() {
		if ( session.getFactory().getSettings().isOrderUpdatesEnabled() ) {
			// sort the updates by fk
			collectionCreations.sort();
			collectionUpdates.sort();
			collectionQueuedOps.sort();
			collectionRemovals.sort();
		}
	}

	public void sortActions() {
		if ( session.getFactory().getSettings().isOrderUpdatesEnabled() ) {
			// sort the updates by pk
			updates.sort();
		}
		if ( session.getFactory().getSettings().isOrderInsertsEnabled() ) {
			insertions.sort();
		}
	}

	public void clearFromFlushNeededCheck(int previousCollectionRemovalSize) {
		collectionCreations.clear();
		collectionUpdates.clear();
		collectionQueuedOps.clear();
		updates.clear();
		// collection deletions are a special case since update() can add
		// deletions of collections not loaded by the session.
		if ( collectionRemovals.size() > previousCollectionRemovalSize ) {
			collectionRemovals.removeLastN( collectionRemovals.size() - previousCollectionRemovalSize );
		}
	}

	public boolean hasAfterTransactionActions() {
		return isTransactionCoordinatorShared ? false : afterTransactionProcesses.hasActions();
	}

	public boolean hasBeforeTransactionActions() {
		return isTransactionCoordinatorShared ? false : beforeTransactionProcesses.hasActions();
	}

	public boolean hasAnyQueuedActions() {
		return !updates.isEmpty() || !insertions.isEmpty() || !unresolvedInsertions.isEmpty() || !deletions.isEmpty() || !collectionUpdates.isEmpty()
				|| !collectionQueuedOps.isEmpty() || !collectionRemovals.isEmpty() || !collectionCreations.isEmpty();
	}

	public void unScheduleDeletion(EntityEntry entry, Object rescuedEntity) {
		if ( rescuedEntity instanceof HibernateProxy ) {
			LazyInitializer initializer = ( ( HibernateProxy ) rescuedEntity ).getHibernateLazyInitializer();
			if ( !initializer.isUninitialized() ) {
				rescuedEntity = initializer.getImplementation( session );
			}
		}
		for ( int i = 0; i < deletions.size(); i++ ) {
			EntityDeleteAction action = deletions.get( i );
			if ( action.getInstance() == rescuedEntity ) {
				deletions.remove( i );
				return;
			}
		}
		for ( int i = 0; i < orphanRemovals.size(); i++ ) {
			EntityDeleteAction action = orphanRemovals.get( i );
			if ( action.getInstance() == rescuedEntity ) {
				orphanRemovals.remove( i );
				return;
			}
		}
		throw new AssertionFailure( "Unable to perform un-delete for instance " + entry.getEntityName() );
	}

	/**
	 * Used by the owning session to explicitly control serialization of the action queue
	 * 
	 * @param oos The stream to which the action queue should get written
	 * @throws IOException Indicates an error writing to the stream
	 */
	public void serialize(ObjectOutputStream oos) throws IOException {
		LOG.trace( "Serializing action-queue" );

		unresolvedInsertions.serialize( oos );

		for ( ExecutableList<?> l : executableLists ) {
			l.writeExternal( oos );
		}
	}

	/**
	 * Used by the owning session to explicitly control deserialization of the action queue.
	 * 
	 * @param ois The stream from which to read the action queue
	 * @param session The session to which the action queue belongs
	 * @return The deserialized action queue
	 * @throws IOException indicates a problem reading from the stream
	 * @throws ClassNotFoundException Generally means we were unable to locate user classes.
	 */
	public static ActionQueue deserialize(ObjectInputStream ois, SessionImplementor session) throws IOException, ClassNotFoundException {
		final boolean traceEnabled = LOG.isTraceEnabled();
		if ( traceEnabled ) {
			LOG.trace( "Deserializing action-queue" );
		}
		ActionQueue rtn = new ActionQueue( session );

		rtn.unresolvedInsertions = UnresolvedEntityInsertActions.deserialize( ois, session );

		for ( ExecutableList<?> l : rtn.executableLists ) {
			l.readExternal( ois );
			if ( traceEnabled ) {
				LOG.tracev( "Deserialized [{0}] entries", l.size() );
			}
			l.afterDeserialize( session );
		}

		return rtn;
	}

	private static abstract class AbstractTransactionCompletionProcessQueue<T> {
		protected SessionImplementor session;
		// Concurrency handling required when transaction completion process is dynamically registered
		// inside event listener (HHH-7478).
		protected Queue<T> processes = new ConcurrentLinkedQueue<T>();

		private AbstractTransactionCompletionProcessQueue(SessionImplementor session) {
			this.session = session;
		}

		public void register(T process) {
			if ( process == null ) {
				return;
			}
			processes.add( process );
		}

		public boolean hasActions() {
			return !processes.isEmpty();
		}
	}

	/**
	 * Encapsulates behavior needed for before transaction processing
	 */
	private static class BeforeTransactionCompletionProcessQueue extends AbstractTransactionCompletionProcessQueue<BeforeTransactionCompletionProcess> {
		private BeforeTransactionCompletionProcessQueue(SessionImplementor session) {
			super( session );
		}

		public void beforeTransactionCompletion() {
			while ( !processes.isEmpty() ) {
				try {
					processes.poll().doBeforeTransactionCompletion( session );
				}
				catch (HibernateException he) {
					throw he;
				}
				catch (Exception e) {
					throw new AssertionFailure( "Unable to perform beforeTransactionCompletion callback", e );
				}
			}
		}
	}

	/**
	 * Encapsulates behavior needed for after transaction processing
	 */
	private static class AfterTransactionCompletionProcessQueue extends AbstractTransactionCompletionProcessQueue<AfterTransactionCompletionProcess> {
		private Set<String> querySpacesToInvalidate = new HashSet<String>();

		private AfterTransactionCompletionProcessQueue(SessionImplementor session) {
			super( session );
		}

		public void addSpaceToInvalidate(String space) {
			querySpacesToInvalidate.add( space );
		}

		public void afterTransactionCompletion(boolean success) {
			while ( !processes.isEmpty() ) {
				try {
					processes.poll().doAfterTransactionCompletion( success, session );
				}
				catch (CacheException ce) {
					LOG.unableToReleaseCacheLock( ce );
					// continue loop
				}
				catch (Exception e) {
					throw new AssertionFailure( "Exception releasing cache locks", e );
				}
			}

			if ( session.getFactory().getSettings().isQueryCacheEnabled() ) {
				session.getFactory().getUpdateTimestampsCache().invalidate(
						querySpacesToInvalidate.toArray( new String[querySpacesToInvalidate.size()] ),
						session
				);
			}
			querySpacesToInvalidate.clear();
		}
	}

	/**
	 * Wrapper class allowing to bind the same transaction completion process queues in different sessions.
	 */
	public static class TransactionCompletionProcesses {
		private final BeforeTransactionCompletionProcessQueue beforeTransactionCompletionProcesses;
		private final AfterTransactionCompletionProcessQueue afterTransactionCompletionProcesses;

		private TransactionCompletionProcesses(
				BeforeTransactionCompletionProcessQueue beforeTransactionCompletionProcessQueue,
				AfterTransactionCompletionProcessQueue afterTransactionCompletionProcessQueue) {
			this.beforeTransactionCompletionProcesses = beforeTransactionCompletionProcessQueue;
			this.afterTransactionCompletionProcesses = afterTransactionCompletionProcessQueue;
		}
	}

	/**
	 * Order the {@link #insertions} queue such that we group inserts against the same entity together (without
	 * violating constraints). The original order is generated by cascade order, which in turn is based on the
	 * directionality of foreign-keys. So even though we will be changing the ordering here, we need to make absolutely
	 * certain that we do not circumvent this FK ordering to the extent of causing constraint violations.
	 * <p>
	 * Sorts the insert actions using more hashes.
	 * </p>
	 * NOTE: this class is not thread-safe.
	 *
	 * @author Jay Erb
	 */
	private static class InsertActionSorter implements ExecutableList.Sorter<AbstractEntityInsertAction> {
		/**
		 * Singleton access
		 */
		public static final InsertActionSorter INSTANCE = new InsertActionSorter();

		private static class BatchIdentifier {

			private final String entityName;
			private final String rootEntityName;

			private Set<String> parentEntityNames = new HashSet<String>( );

			private Set<String> childEntityNames = new HashSet<String>( );

			private BatchIdentifier parent;

			BatchIdentifier(String entityName, String rootEntityName) {
				this.entityName = entityName;
				this.rootEntityName = rootEntityName;
			}

			public BatchIdentifier getParent() {
				return parent;
			}

			public void setParent(BatchIdentifier parent) {
				this.parent = parent;
			}

			@Override
			public boolean equals(Object o) {
				if ( this == o ) {
					return true;
				}
				if ( !( o instanceof BatchIdentifier ) ) {
					return false;
				}
				BatchIdentifier that = (BatchIdentifier) o;
				return (entityName == that.entityName) || (entityName != null && entityName.equals(that.entityName));
			}

			@Override
			public int hashCode() {
				return entityName != null ? entityName.hashCode() : 0;
			}

			String getEntityName() {
				return entityName;
			}

			String getRootEntityName() {
				return rootEntityName;
			}

			Set<String> getParentEntityNames() {
				return parentEntityNames;
			}

			Set<String> getChildEntityNames() {
				return childEntityNames;
			}

			boolean hasAnyParentEntityNames(BatchIdentifier batchIdentifier) {
				return parentEntityNames.contains( batchIdentifier.getEntityName() ) ||
						parentEntityNames.contains( batchIdentifier.getRootEntityName() );
			}

			boolean hasAnyChildEntityNames(BatchIdentifier batchIdentifier) {
				return childEntityNames.contains( batchIdentifier.getEntityName() );
			}

			/**
			 * Check if the this {@link BatchIdentifier} has a parent or grand parent
			 * matching the given {@link BatchIdentifier reference.
			 *
			 * @param batchIdentifier {@link BatchIdentifier} reference
			 *
			 * @return This {@link BatchIdentifier} has a parent matching the given {@link BatchIdentifier reference
			 */
			boolean hasParent(BatchIdentifier batchIdentifier) {
				return (
						parent == batchIdentifier
								|| parentEntityNames.contains( batchIdentifier.getEntityName() )
								|| ( parentEntityNames.contains( batchIdentifier.getRootEntityName() ) && !this.getEntityName().equals( batchIdentifier.getRootEntityName() ) )
								|| parent != null && parent.hasParent( batchIdentifier, new ArrayList<BatchIdentifier>() )
				);
			}

			private boolean hasParent(BatchIdentifier batchIdentifier, List<BatchIdentifier> stack) {
				if ( !stack.contains( this ) && parent != null ) {
					stack.add( this );
					return parent.hasParent( batchIdentifier, stack );
				}
				return (
						parent == batchIdentifier
								|| parentEntityNames.contains( batchIdentifier.getEntityName() )
				);
			}
		}

		// the mapping of entity names to their latest batch numbers.
		private List<BatchIdentifier> latestBatches;

		// the map of batch numbers to EntityInsertAction lists
		private Map<BatchIdentifier, List<AbstractEntityInsertAction>> actionBatches;

		public InsertActionSorter() {
		}

		/**
		 * Sort the insert actions.
		 */
		public void sort(List<AbstractEntityInsertAction> insertions) {
			// optimize the hash size to eliminate a rehash.
			this.latestBatches = new ArrayList<BatchIdentifier>( );
			this.actionBatches = new HashMap<BatchIdentifier, List<AbstractEntityInsertAction>>();

			for ( AbstractEntityInsertAction action : insertions ) {
				BatchIdentifier batchIdentifier = new BatchIdentifier(
						action.getEntityName(),
						action.getSession()
						      .getFactory()
						      .getEntityPersister( action.getEntityName() )
						      .getRootEntityName()
				);

				// the entity associated with the current action.
				Object currentEntity = action.getInstance();
				int index = latestBatches.indexOf( batchIdentifier );

				if ( index != -1 )  {
					batchIdentifier = latestBatches.get( index );
				}
				else {
					latestBatches.add( batchIdentifier );
				}
				addParentChildEntityNames( action, batchIdentifier );
				addToBatch( batchIdentifier, action );
			}

			// Examine each entry in the batch list, and build the dependency graph.
			for ( int i = 0; i < latestBatches.size(); i++ ) {
				BatchIdentifier batchIdentifier = latestBatches.get( i );

				for ( int j = i - 1; j >= 0; j-- ) {
					BatchIdentifier prevBatchIdentifier = latestBatches.get( j );
					if ( prevBatchIdentifier.hasAnyParentEntityNames( batchIdentifier ) ) {
						prevBatchIdentifier.parent = batchIdentifier;
					}
					if ( batchIdentifier.hasAnyChildEntityNames( prevBatchIdentifier ) ) {
						prevBatchIdentifier.parent = batchIdentifier;
					}
				}

				for ( int j = i + 1; j < latestBatches.size(); j++ ) {
					BatchIdentifier nextBatchIdentifier = latestBatches.get( j );

					if ( nextBatchIdentifier.hasAnyParentEntityNames( batchIdentifier ) ) {
						nextBatchIdentifier.parent = batchIdentifier;
					}
					if ( batchIdentifier.hasAnyChildEntityNames( nextBatchIdentifier ) ) {
						nextBatchIdentifier.parent = batchIdentifier;
					}
				}
			}

			boolean sorted = false;

			long maxIterations = latestBatches.size() * latestBatches.size();
			long iterations = 0;

			sort:
			do {
				// Examine each entry in the batch list, sorting them based on parent/child association
				// as depicted by the dependency graph.
				iterations++;

				for ( int i = 0; i < latestBatches.size(); i++ ) {
					BatchIdentifier batchIdentifier = latestBatches.get( i );

					// Iterate next batches and make sure that children types are after parents.
					// Since the outer loop looks at each batch entry individually and the prior loop will reorder
					// entries as well, we need to look and verify if the current batch is a child of the next
					// batch or if the current batch is seen as a parent or child of the next batch.
					for ( int j = i + 1; j < latestBatches.size(); j++ ) {
						BatchIdentifier nextBatchIdentifier = latestBatches.get( j );

						if ( batchIdentifier.hasParent( nextBatchIdentifier ) ) {
							if( nextBatchIdentifier.hasParent( batchIdentifier ) ) {
								//cycle detected, no need to continue
								break sort;
							}

							latestBatches.remove( batchIdentifier );
							latestBatches.add( j, batchIdentifier );

							continue sort;
						}
					}
				}
				sorted = true;
			}
			while ( !sorted && iterations <= maxIterations);

			if ( iterations > maxIterations ) {
				LOG.warn( "The batch containing " + latestBatches.size() + " statements could not be sorted after " + maxIterations + " iterations. " +
						"This might indicate a circular entity relationship." );
			}

			// Now, rebuild the insertions list. There is a batch for each entry in the name list.
			if ( sorted ) {
				insertions.clear();

				for ( BatchIdentifier rootIdentifier : latestBatches ) {
					List<AbstractEntityInsertAction> batch = actionBatches.get( rootIdentifier );
					insertions.addAll( batch );
				}
			}
		}

		/**
		 * Add parent and child entity names so that we know how to rearrange dependencies
		 *
		 * @param action The action being sorted
		 * @param batchIdentifier The batch identifier of the entity affected by the action
		 */
		private void addParentChildEntityNames(AbstractEntityInsertAction action, BatchIdentifier batchIdentifier) {
			Object[] propertyValues = action.getState();
			EntityMetamodel entityMetamodel = action.getPersister().getEntityMetamodel();
			if ( entityMetamodel != null ) {
				Type[] propertyTypes = entityMetamodel.getPropertyTypes();
				Type identifierType = entityMetamodel.getIdentifierProperty().getType();

				for ( int i = 0; i < propertyValues.length; i++ ) {
					Object value = propertyValues[i];
					Type type = propertyTypes[i];
					addParentChildEntityNameByPropertyAndValue( action, batchIdentifier, type, value );
				}

				if ( identifierType.isComponentType() ) {
					CompositeType compositeType = (CompositeType) identifierType;
					Type[] compositeIdentifierTypes = compositeType.getSubtypes();

					for ( Type type : compositeIdentifierTypes ) {
						addParentChildEntityNameByPropertyAndValue( action, batchIdentifier, type, null );
					}
				}
			}
		}

		private void addParentChildEntityNameByPropertyAndValue(AbstractEntityInsertAction action, BatchIdentifier batchIdentifier, Type type, Object value) {
			if ( type.isEntityType() ) {
				final EntityType entityType = (EntityType) type;
				final String entityName = entityType.getName();
				final String rootEntityName = action.getSession().getFactory().getEntityPersister( entityName ).getRootEntityName();

				if ( entityType.isOneToOne() && OneToOneType.class.cast( entityType ).getForeignKeyDirection().equals( ForeignKeyDirection.FOREIGN_KEY_TO_PARENT ) ) {
					if ( !entityType.isReferenceToPrimaryKey() ) {
						batchIdentifier.getChildEntityNames().add( entityName );
					}
					if ( !rootEntityName.equals( entityName ) ) {
						batchIdentifier.getChildEntityNames().add( rootEntityName );
					}
				}
				else {
					if ( !batchIdentifier.getEntityName().equals( entityName ) ) {
						batchIdentifier.getParentEntityNames().add( entityName );
					}
					if ( value != null ) {
						String valueClass = value.getClass().getName();
						if ( !valueClass.equals( entityName ) ) {
							batchIdentifier.getParentEntityNames().add( valueClass );
						}
					}
					if ( !rootEntityName.equals( entityName ) ) {
						batchIdentifier.getParentEntityNames().add( rootEntityName );
					}
				}
			}
			else if ( type.isCollectionType() ) {
				CollectionType collectionType = (CollectionType) type;
				final SessionFactoryImplementor sessionFactory = ( (SessionImplementor) action.getSession() )
						.getFactory();
				if ( collectionType.getElementType( sessionFactory ).isEntityType() &&
						!sessionFactory.getCollectionPersister( collectionType.getRole() ).isManyToMany() ) {
					String entityName = collectionType.getAssociatedEntityName( sessionFactory );
					String rootEntityName = action.getSession().getFactory().getEntityPersister( entityName ).getRootEntityName();
					batchIdentifier.getChildEntityNames().add( entityName );
					if ( !rootEntityName.equals( entityName ) ) {
						batchIdentifier.getChildEntityNames().add( rootEntityName );
					}
				}
			}
			else if ( type.isComponentType() && value != null ) {
				// Support recursive checks of composite type properties for associations and collections.
				CompositeType compositeType = (CompositeType) type;
				final SessionImplementor session = action.getSession();
				Object[] componentValues = compositeType.getPropertyValues( value, session );
				for ( int j = 0; j < componentValues.length; ++j ) {
					Type componentValueType = compositeType.getSubtypes()[j];
					Object componentValue = componentValues[j];
					addParentChildEntityNameByPropertyAndValue( action, batchIdentifier, componentValueType, componentValue );
				}
			}
		}

		private void addToBatch(BatchIdentifier batchIdentifier, AbstractEntityInsertAction action) {
			List<AbstractEntityInsertAction> actions = actionBatches.get( batchIdentifier );

			if ( actions == null ) {
				actions = new LinkedList<AbstractEntityInsertAction>();
				actionBatches.put( batchIdentifier, actions );
			}
			actions.add( action );
		}

	}

}
